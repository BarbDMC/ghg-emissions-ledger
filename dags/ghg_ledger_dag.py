"""Monthly ingestion of energy invoices and emission factors into the GHG ledger.

Local files -> MinIO landing zone -> PostgreSQL ``raw`` schema (append-only) -> dbt.

Each run targets one warehouse through the ``target`` param: ``local`` (the Docker
Postgres) or ``supabase`` (cloud Postgres, configured with SUPABASE_* in .env).
The run applies pending migrations, appends the landed files and builds the dbt
models, so either warehouse ends up with ``marts.fct_emissions_ledger``.

Raw tables are never truncated, deleted from or updated: every run appends the
landed files with their ``source_file`` and a UTC ``ingestion_timestamp``, so the
full history of what was received is preserved. Deduplication (e.g. corrected
invoices re-issued with the same ``invoice_id``) belongs to the dbt staging layer.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pendulum
import psycopg2
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from minio import Minio

log = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("GHG_DATA_DIR", "/opt/airflow/data"))
MIGRATIONS_DIR = Path(os.getenv("GHG_MIGRATIONS_DIR", "/opt/airflow/migrations"))
DBT_PROJECT_DIR = os.getenv("GHG_DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_BIN = os.getenv("GHG_DBT_BIN", "/home/airflow/dbt_venv/bin/dbt")

BUCKET = os.getenv("GHG_MINIO_BUCKET", "climate-invoices")
INVOICES_PREFIX = "landing/invoices/"
FACTORS_KEY = "landing/factors/emission_factors.csv"

TARGETS = ["local", "supabase"]
DEFAULT_TARGET = os.getenv("WAREHOUSE_TARGET") or "local"
SUPABASE_REQUIRED = ["SUPABASE_HOST", "SUPABASE_USER", "SUPABASE_PASSWORD"]

INVOICE_COLUMNS = [
    "invoice_id", "office_id", "country_code", "issue_date", "consumption_start_date",
    "consumption_end_date", "energy_type", "usage_amount", "usage_unit", "cost_amount",
    "currency",
]
FACTOR_COLUMNS = [
    "factor_id", "country_code", "energy_type", "activity_unit", "co2e_factor",
    "source_dataset", "factor_year", "retrieved_at",
]
AUDIT_COLUMNS = ["source_file", "ingestion_timestamp"]


def current_target() -> str:
    return get_current_context()["params"]["target"]


def warehouse_settings(target: str) -> dict:
    """psycopg2 connection settings for ``target``; fails fast if Supabase is not configured."""
    if target == "local":
        return {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname": os.getenv("POSTGRES_DB", "warehouse"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "sslmode": "disable",
        }
    if target == "supabase":
        missing = [name for name in SUPABASE_REQUIRED if not os.getenv(name)]
        if missing:
            raise ValueError(f"Target 'supabase' is not configured: set {', '.join(missing)} "
                             "in .env and recreate the Airflow container.")
        return {
            "host": os.environ["SUPABASE_HOST"],
            "port": int(os.getenv("SUPABASE_PORT") or "5432"),
            "dbname": os.getenv("SUPABASE_DB") or "postgres",
            "user": os.environ["SUPABASE_USER"],
            "password": os.environ["SUPABASE_PASSWORD"],
            "sslmode": os.getenv("SUPABASE_SSLMODE") or "require",
        }
    raise ValueError(f"Unknown target {target!r}; expected one of {TARGETS}")


def connect(target: str):
    settings = warehouse_settings(target)
    log.info("Connecting to %s warehouse at %s:%s/%s", target, settings["host"],
             settings["port"], settings["dbname"])
    return psycopg2.connect(**settings, connect_timeout=15, application_name="ghg_ledger_dag")


def minio_client() -> Minio:
    return Minio(
        os.getenv("GHG_MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("GHG_MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("GHG_MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


def apply_migrations(conn) -> list[str]:
    """Apply each pending migrations/*.sql once, in order, recording it in schema_migrations."""
    with conn, conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.schema_migrations (
                version VARCHAR(100) PRIMARY KEY,
                applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )""")
        cursor.execute("SELECT version FROM public.schema_migrations")
        applied = {row[0] for row in cursor.fetchall()}

    newly_applied = []
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if path.stem in applied:
            continue
        with conn, conn.cursor() as cursor:
            cursor.execute(path.read_text())
            cursor.execute("INSERT INTO public.schema_migrations (version) VALUES (%s)",
                           (path.stem,))
        log.info("Applied migration %s", path.name)
        newly_applied.append(path.stem)
    if not newly_applied:
        log.info("No pending migrations (%d already applied)", len(applied))
    return newly_applied


def append_object(cursor, client: Minio, key: str, table: str, columns: list[str],
                  ingested_at: str) -> int:
    """Stream one CSV object from MinIO and COPY it into ``table`` with audit columns."""
    source_file = f"s3://{BUCKET}/{key}"
    response = client.get_object(BUCKET, key)
    # urllib3 closes the stream at EOF by default, which breaks io.TextIOWrapper's final read.
    response.auto_close = False
    try:
        reader = csv.reader(io.TextIOWrapper(response, encoding="utf-8", newline=""))
        header = next(reader, None)
        if header != columns:
            raise ValueError(f"{source_file}: unexpected header {header}, expected {columns}")

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        rows = 0
        for row in reader:
            writer.writerow([*row, source_file, ingested_at])
            rows += 1
    finally:
        response.close()
        response.release_conn()

    buffer.seek(0)
    cursor.copy_expert(
        f"COPY {table} ({', '.join(columns + AUDIT_COLUMNS)}) FROM STDIN WITH (FORMAT csv)",
        buffer,
    )
    log.info("Appended %d rows from %s into %s", rows, source_file, table)
    return rows


def append_objects(target: str, keys: list[str], table: str, columns: list[str]) -> int:
    """Append all objects in a single transaction, so a failed run leaves no partial load."""
    if not keys:
        raise ValueError(f"No objects found in s3://{BUCKET} for {table}")
    client = minio_client()
    ingested_at = datetime.now(timezone.utc).isoformat()
    conn = connect(target)
    try:
        with conn, conn.cursor() as cursor:
            total = sum(append_object(cursor, client, key, table, columns, ingested_at)
                        for key in keys)
    finally:
        conn.close()
    log.info("Committed %d rows into %s on %s (ingestion_timestamp=%s)",
             total, table, target, ingested_at)
    return total


@dag(
    dag_id="ghg_corporate_ledger_ingestion",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 1,
                  "retry_delay": pendulum.duration(minutes=2)},
    params={
        "target": Param(DEFAULT_TARGET, type="string", enum=TARGETS,
                        description="Warehouse to load: local Docker Postgres or Supabase."),
    },
    tags=["ghg", "ingestion", "raw", "dbt"],
    doc_md=__doc__,
)
def ghg_corporate_ledger_ingestion():
    @task
    def run_migrations() -> list[str]:
        conn = connect(current_target())
        try:
            return apply_migrations(conn)
        finally:
            conn.close()

    @task
    def upload_to_minio() -> dict:
        client = minio_client()
        if not client.bucket_exists(BUCKET):
            client.make_bucket(BUCKET)

        invoice_files = sorted((DATA_DIR / "mock_invoices").glob("*.csv"))
        if not invoice_files:
            raise FileNotFoundError(f"No invoice CSVs in {DATA_DIR / 'mock_invoices'}")
        uploads = {f"{INVOICES_PREFIX}{path.name}": path for path in invoice_files}
        uploads[FACTORS_KEY] = DATA_DIR / "emission_factors.csv"

        for key, path in uploads.items():
            client.fput_object(BUCKET, key, str(path), content_type="text/csv")
            log.info("Uploaded %s -> s3://%s/%s", path, BUCKET, key)
        return {"invoices": len(invoice_files), "factors": 1}

    @task
    def ingest_factors_to_postgres() -> int:
        return append_objects(current_target(), [FACTORS_KEY], "raw.emission_factors",
                              FACTOR_COLUMNS)

    @task
    def ingest_invoices_to_postgres() -> int:
        keys = sorted(obj.object_name for obj in minio_client().list_objects(
            BUCKET, prefix=INVOICES_PREFIX, recursive=True)
            if obj.object_name.endswith(".csv"))
        return append_objects(current_target(), keys, "raw.invoices", INVOICE_COLUMNS)

    @task
    def run_dbt() -> None:
        """``dbt build`` (models + tests) on the target; profiles.yml reads WAREHOUSE_* vars.

        Credentials go through the subprocess env, so they never appear in rendered
        templates or the command line.
        """
        target = current_target()
        settings = warehouse_settings(target)
        env = {
            **os.environ,
            "WAREHOUSE_HOST": settings["host"],
            "WAREHOUSE_PORT": str(settings["port"]),
            "WAREHOUSE_DB": settings["dbname"],
            "WAREHOUSE_USER": settings["user"],
            "WAREHOUSE_PASSWORD": settings["password"],
            "WAREHOUSE_SSLMODE": settings["sslmode"],
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }
        work_dir = f"/tmp/dbt/{target}"
        command = [
            DBT_BIN, "build", "--no-use-colors",
            "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROJECT_DIR,
            "--target-path", f"{work_dir}/target", "--log-path", f"{work_dir}/logs",
        ]
        with subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True) as process:
            for line in process.stdout:
                log.info(line.rstrip())
        if process.returncode != 0:
            raise RuntimeError(f"dbt build failed on {target} (exit code {process.returncode})")

    loads = [ingest_factors_to_postgres(), ingest_invoices_to_postgres()]
    run_migrations() >> upload_to_minio() >> loads >> run_dbt()


ghg_corporate_ledger_ingestion()
