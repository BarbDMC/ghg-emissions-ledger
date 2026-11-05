"""Monthly ingestion of energy invoices and emission factors into the GHG ledger.

Local files -> MinIO landing zone -> PostgreSQL ``raw`` schema (append-only).

Raw tables are never truncated, deleted from or updated: every run appends the
landed files with their ``source_file`` and a UTC ``ingestion_timestamp``, so the
full history of what was received is preserved. Deduplication (e.g. corrected
invoices re-issued with the same ``invoice_id``) belongs to the staging layer.
"""

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from minio import Minio

log = logging.getLogger(__name__)

POSTGRES_CONN_ID = "ghg_warehouse"
DATA_DIR = Path(os.getenv("GHG_DATA_DIR", "/opt/airflow/data"))
DDL_FILE = Path(os.getenv("GHG_RAW_DDL_FILE", "/opt/airflow/sql/init_raw_tables.sql"))

BUCKET = os.getenv("GHG_MINIO_BUCKET", "climate-invoices")
INVOICES_PREFIX = "landing/invoices/"
FACTORS_KEY = "landing/factors/emission_factors.csv"

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


def minio_client() -> Minio:
    return Minio(
        os.getenv("GHG_MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("GHG_MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("GHG_MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


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


def append_objects(keys: list[str], table: str, columns: list[str]) -> int:
    """Append all objects in a single transaction, so a failed run leaves no partial load."""
    if not keys:
        raise ValueError(f"No objects found in s3://{BUCKET} for {table}")
    client = minio_client()
    ingested_at = datetime.now(timezone.utc).isoformat()
    with PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn() as conn:
        with conn.cursor() as cursor:
            total = sum(append_object(cursor, client, key, table, columns, ingested_at)
                        for key in keys)
    log.info("Committed %d rows into %s (ingestion_timestamp=%s)", total, table, ingested_at)
    return total


@dag(
    dag_id="ghg_corporate_ledger_ingestion",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 1,
                  "retry_delay": pendulum.duration(minutes=2)},
    tags=["ghg", "ingestion", "raw"],
    doc_md=__doc__,
)
def ghg_corporate_ledger_ingestion():
    @task
    def setup_raw_tables() -> None:
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run(DDL_FILE.read_text())

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
        return append_objects([FACTORS_KEY], "raw.emission_factors", FACTOR_COLUMNS)

    @task
    def ingest_invoices_to_postgres() -> int:
        keys = sorted(obj.object_name for obj in minio_client().list_objects(
            BUCKET, prefix=INVOICES_PREFIX, recursive=True)
            if obj.object_name.endswith(".csv"))
        return append_objects(keys, "raw.invoices", INVOICE_COLUMNS)

    uploaded = upload_to_minio()
    setup_raw_tables() >> uploaded >> [ingest_factors_to_postgres(), ingest_invoices_to_postgres()]


ghg_corporate_ledger_ingestion()
