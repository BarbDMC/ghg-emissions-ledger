# GHG Emissions Ledger

A local, end-to-end data engineering project that turns corporate energy invoices into an auditable **Scope 1 & 2 carbon ledger**: emission factors from an API, object storage, orchestration, an append-only warehouse, tested dbt transformations and an ESG dashboard, all running on a laptop with Docker.

It covers five offices (Quito, New York, London, Frankfurt and Paris) with 12 months of electricity and diesel invoices, priced in USD, GBP and EUR and measured in kWh, liters and US gallons. Emission factors come from the [Climatiq](https://www.climatiq.io/) API, with official reference factors (EPA, DESNZ/DEFRA, Ember) as a fallback.

The same pipeline can load a cloud database on [Supabase](https://supabase.com/), a hosted PostgreSQL, so the dashboard can be shared on Streamlit Community Cloud without anyone running Docker.

## The problem

Corporate carbon accounting starts from invoices that were never meant for it:

- Each country reports consumption in its own units (liters in Europe, gallons in the Americas) and bills in its own currency.
- Utilities **re-issue corrected invoices** with the same `invoice_id` and a later date. Counting both would overstate emissions.
- The same kWh emits very different CO₂e depending on the grid: about **10× more in Frankfurt than in Paris**.
- Auditors need to trace every tonne back to its invoice, file and emission factor source.

The goal is to answer, reliably and repeatably:

- How many tonnes of CO₂e did each office emit, and how much is Scope 1 vs. Scope 2?
- How do emissions evolve month by month?
- Which offices sit on the most carbon-intensive grids?
- Which invoice, factor and source file produced each number?

## Scope 1 and Scope 2

Both are categories of the [GHG Protocol](https://ghgprotocol.org/), the standard for corporate carbon accounting:

| Scope | Meaning | In this project |
|---|---|---|
| **Scope 1** | Direct emissions from sources the company owns or controls | Diesel burned in each office's backup generators |
| **Scope 2** | Indirect emissions from the generation of purchased electricity | Office electricity × the country's grid factor (location-based method) |

Scope 3 (the rest of the value chain: suppliers, travel, freight) is out of scope.

## Architecture

```
  scripts/generate_mock_invoices.py ──► data/mock_invoices/*.csv
  scripts/fetch_emission_factors.py ──► data/emission_factors.csv   (Climatiq API, reference fallback)
                │
                │   Airflow DAG: ghg_corporate_ledger_ingestion   (target = local | supabase)
                ▼
  ┌─────────────────┐   upload_to_minio
  │ MinIO           │   s3://climate-invoices/landing/invoices/*.csv
  │ (data lake)     │   s3://climate-invoices/landing/factors/emission_factors.csv
  └────────┬────────┘
           │   ingest_*_to_postgres (streamed COPY, append-only)
           ▼
  ┌──────────────────────┐
  │ PostgreSQL 15        │   raw.*           ← exact copy of the CSVs (all TEXT) + audit columns
  │ local "warehouse"    │   staging.*       ← dbt views: typed, normalized units
  │ or Supabase          │   intermediate.*  ← dbt view: latest version of each invoice
  │                      │   marts.*         ← dbt table: fct_emissions_ledger
  └────────┬─────────────┘
           ▼
  ┌─────────────────┐
  │ Streamlit app   │   esg_dashboard.py
  └─────────────────┘
```

The DAG runs five tasks:

1. **`run_migrations`** applies each pending `.sql` file in `migrations/` once, in order, and records it in `public.schema_migrations`. On an empty database (for example a new Supabase project) this creates the schemas, the raw tables and their append-only triggers.
2. **`upload_to_minio`** uploads the invoice CSVs and the emission factors file to the `climate-invoices` bucket under `landing/`.
3. **`ingest_factors_to_postgres`** and **`ingest_invoices_to_postgres`** stream each file from MinIO into Postgres with `COPY`. Every row gets its `source_file` (the `s3://` URI) and a UTC `ingestion_timestamp`, and all files of a task load in one transaction, so a failed run leaves no partial data.
4. **`run_dbt`** runs `dbt build` (models and tests) inside the Airflow container against the same warehouse. A failing data test fails the run.

Each run picks its warehouse with the DAG's `target` parameter: `local` for the Postgres container, or `supabase` for the cloud database (see [Using Supabase](#using-supabase-as-the-warehouse)).

## Tech stack

| Layer | Tool | Notes |
|---|---|---|
| Containers | Docker Compose (Colima on macOS) | One network, `ghg-network`, for all services |
| Emission factors | Climatiq API (Search + Estimate) | Reference factors from EPA, DESNZ/DEFRA and Ember as fallback |
| Data lake | MinIO via `pgsty/silo` | Community fork; official MinIO images are gone (see [FAQ](#why-pgstysilo-instead-of-the-official-minio-image)) |
| Orchestration | Apache Airflow 2.8.1, LocalExecutor | Custom image with `minio`, `psycopg2-binary` and dbt |
| Warehouse | PostgreSQL 15 (local) or Supabase | Chosen per run with the DAG's `target` parameter |
| Transformations | dbt-core 1.10 + dbt-postgres 1.9 | Own virtualenv inside the Airflow image |
| Dashboard | Streamlit 1.50 + Plotly | Reads `marts.fct_emissions_ledger` |

## Data model

**Raw** (`raw` schema, tables) stores every column as `TEXT`, exactly as received, plus `source_file` and `ingestion_timestamp`. Triggers reject `UPDATE`, `DELETE` and `TRUNCATE`, so the full history of what was received is kept.

**Staging** (`staging` schema, views):

- `stg_invoices` casts dates and amounts and normalizes units: `gal`/`gallons` → `gallon`, `L`/`liters` → `liter`, and `MWh` → `kWh` (×1000).
- `stg_emission_factors` casts the factor to `NUMERIC(12, 6)` and keeps the latest ingested factor per country and energy type. Without that, every re-run of the DAG would duplicate the factors and multiply rows in the ledger join.

**Intermediate** (`intermediate` schema, view): `int_invoices_deduplicated` keeps one row per `invoice_id`, the latest by `ingestion_timestamp` and then `issue_date`. Corrected invoices replace their originals.

**Mart** (`marts` schema, table):

| Table | Grain | Rows |
|---|---|---|
| `fct_emissions_ledger` | One row per invoice (latest version) | 120 |

Key columns: `ledger_id` (`MD5(invoice_id)`), `reporting_year` / `reporting_month` (from the consumption start date), `ghg_scope`, `activity_amount` / `activity_unit`, `co2e_factor`, `factor_source`, `co2e_kg`, `co2e_tonnes`, `cost_amount`, `currency`.

With the bundled data the ledger totals **464.43 t CO₂e** from November 2025 to October 2026, **87.4% of it Scope 2**.

### Data quality

`dbt build` runs 25 data tests alongside the 4 models:

- `unique` and `not_null` on `ledger_id` and `invoice_id`, so deduplication can never let duplicates into the mart.
- `accepted_values` on `ghg_scope`, energy types and normalized units.
- `assert_no_negative_emissions_or_usage`: no negative consumption or CO₂e.
- `assert_ledger_covers_all_invoices`: the ledger's inner join can't silently drop an invoice that has no matching factor.
- `assert_activity_unit_matches_factor_unit`: liters are never multiplied by a per-gallon factor.

`data/bad_data/corrupt_invoice.csv` holds a negative consumption and an empty `invoice_id`, to show the suite catching bad data (see [Testing with bad data](#testing-with-bad-data)).

## Getting started

### Prerequisites

- Docker with the Compose v2 plugin (`docker compose version` must work). On macOS with Colima, give the VM at least 4 CPUs and 8 GB of RAM.
- Python 3.9+ for the scripts, dbt and Streamlit.
- Optional: a free [Climatiq](https://www.climatiq.io/) API key. Without it the factors script uses the reference factors.

### 1. Set up the environment

```bash
cp .env.example .env
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

If a local PostgreSQL already uses port 5432, set `POSTGRES_HOST_PORT=5433` in `.env`. Containers keep talking to each other on 5432.

### 2. Generate the data (optional)

The repository already includes the invoices and emission factors in `data/`. To regenerate them:

```bash
.venv/bin/python scripts/generate_mock_invoices.py     # 12 months x 5 offices x 2 energy types
.venv/bin/python scripts/fetch_emission_factors.py     # Climatiq, or reference factors
```

- The invoice generator is seeded (`--seed`, default 42), so it always produces the same data. It also writes one corrected invoice: same `invoice_id`, issued 15 days later with a different `usage_amount`.
- The factors script reads `CLIMATIQ_API_KEY` from `.env`. If the key is missing, the API is unreachable or the quota is exhausted, it logs a warning and writes the reference factors, so the pipeline never stops. Use `--offline` to skip the API.

### 3. Start the stack

```bash
docker compose up -d --build
```

On first start, `createbuckets` creates the `climate-invoices` bucket and exits, and Airflow migrates its metadata database and creates the admin user.

| Service | URL / address | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| MinIO S3 API | http://localhost:9000 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 (or `POSTGRES_HOST_PORT`), database `warehouse` | postgres / postgres |

### 4. Run the pipeline

In the Airflow UI, turn on the `ghg_corporate_ledger_ingestion` DAG and use **Trigger DAG w/ config**. The form asks for a `target`: keep `local` for the Postgres container. When all five tasks are green, the `raw`, `staging`, `intermediate` and `marts` schemas are built and tested.

The DAG is scheduled `@monthly` with `catchup=False`. `WAREHOUSE_TARGET` in `.env` sets the default target for scheduled and manual runs.

To run dbt from the host instead, from `dbt_project/`:

```bash
WAREHOUSE_PORT=5433 ../.venv/bin/dbt build --profiles-dir .
```

`dbt_project/profiles.yml` reads its connection from `WAREHOUSE_*` variables, which the DAG sets for the chosen target, so it contains no credentials. Without them it falls back to the local container on `localhost:5432`.

### 5. Run the dashboard

```bash
.venv/bin/streamlit run esg_dashboard.py --server.headless true --server.address localhost
```

Open http://localhost:8501. A **Data source** badge under the title shows which warehouse it's reading.

The dashboard follows the same `WAREHOUSE_TARGET` as the DAG and re-reads `.env` on every interaction, so changing it doesn't need a restart. If it's `supabase` but Supabase isn't configured, can't be reached or has no ledger yet, the dashboard falls back to the local warehouse and explains why in the sidebar. `.streamlit/secrets.toml.example` shows how to define the connections explicitly instead. `secrets.toml` is in `.gitignore`.

## Using Supabase as the warehouse

1. Create a Supabase project for this ledger, open **Connect** and copy the **Session pooler** details. The direct `db.<ref>.supabase.co` host is IPv6-only, which Docker can't reach.
2. Fill in the Supabase block in `.env`:

   ```bash
   WAREHOUSE_TARGET=supabase
   SUPABASE_HOST=aws-0-<region>.pooler.supabase.com
   SUPABASE_PORT=5432
   SUPABASE_DB=postgres
   SUPABASE_USER=postgres.<project-ref>
   SUPABASE_PASSWORD=<your database password>
   SUPABASE_SSLMODE=require
   ```

3. Recreate Airflow so it picks up the new values. Docker only reads `.env` when a container is created:

   ```bash
   docker compose up -d
   ```

4. Trigger the DAG with `target` = `supabase`. The first run applies both migrations on the empty database, loads the raw tables and builds the dbt models there.
5. Refresh the dashboard: the badge switches to **Data source: supabase**.

Both warehouses stay available: switching is just a different `target` on the next run. If the Supabase values are missing, a `supabase` run fails on its first task with a message saying which ones. The whole ledger takes well under 1 MB, far from the free plan's 500 MB.

## Publishing the dashboard on Streamlit Community Cloud

The public app reads Supabase only, so it needs no Docker.

1. **Create a read-only role.** The app's secrets should never hold the `postgres` password. Run this once in the Supabase SQL Editor, with a strong password of your own:

   ```sql
   CREATE ROLE ghg_reader WITH LOGIN PASSWORD '<strong password>';
   GRANT USAGE ON SCHEMA marts TO ghg_reader;
   GRANT SELECT ON ALL TABLES IN SCHEMA marts TO ghg_reader;
   -- dbt recreates the table on every run; this keeps new ones readable
   ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA marts GRANT SELECT ON TABLES TO ghg_reader;
   ```

2. **Deploy:** on [share.streamlit.io](https://share.streamlit.io), click **Create app** and pick this repository, branch `main` and file `esg_dashboard.py`. The root `requirements.txt` lists the dashboard's packages.
3. **Add the secrets:** under **Advanced settings → Secrets**, paste only a Supabase section:

   ```toml
   [connections.supabase]
   dialect = "postgresql"
   driver = "psycopg2"
   host = "aws-0-<region>.pooler.supabase.com"
   port = 5432
   database = "postgres"
   username = "ghg_reader.<project-ref>"
   password = "<ghg_reader password>"
   query = { sslmode = "require" }
   ```

   The app also accepts the same names as `.env` as top-level secrets (`SUPABASE_HOST = "..."`, `SUPABASE_USER`, `SUPABASE_PASSWORD`). `SUPABASE_PORT`, `SUPABASE_DB` and `SUPABASE_SSLMODE` default to `5432`, `postgres` and `require`.

With Supabase configured and no `WAREHOUSE_TARGET`, the app reads Supabase and shows "Data source: supabase". Query results are cached for 10 minutes, so a new DAG run shows up within that window.

## Testing with bad data

`raw` is append-only, so bad rows can't be inserted and then deleted. To test the suite, run dbt against a sandbox copy of `raw` instead. The source schema is a dbt variable (`raw_schema`, default `raw`):

1. Create a `raw_qa` schema with a copy of `raw.invoices` (without the triggers) and a view over `raw.emission_factors`, then load `data/bad_data/corrupt_invoice.csv` into it.
2. Build and test against the sandbox:

   ```bash
   WAREHOUSE_PORT=5433 ../.venv/bin/dbt run  --profiles-dir . --vars '{raw_schema: raw_qa}'
   WAREHOUSE_PORT=5433 ../.venv/bin/dbt test --profiles-dir . --vars '{raw_schema: raw_qa}'
   ```

   `dbt test` reports 7 failures: the negative-emissions test, the `not_null` checks from source to mart (`MD5(NULL)` also leaves `ledger_id` empty) and the coverage test, because `NULL` never matches in a join.
3. Drop `raw_qa` and run `dbt run` and `dbt test` without `--vars` to rebuild from the real `raw`. All tests pass again.

## Project structure

```
.
├── esg_dashboard.py                  # Streamlit dashboard
├── dags/
│   └── ghg_ledger_dag.py             # Migrations → MinIO → append-only COPY → dbt build
├── migrations/
│   ├── 001_create_schemas.sql        # raw/staging/intermediate/marts + schema_migrations
│   └── 002_create_raw_tables.sql     # raw tables + append-only triggers
├── scripts/
│   ├── generate_mock_invoices.py     # Seeded synthetic invoices (incl. a corrected one)
│   └── fetch_emission_factors.py     # Climatiq factors with reference fallback
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml                  # Reads WAREHOUSE_* env vars, no credentials
│   ├── macros/                       # generate_schema_name, generic unique-combination test
│   ├── models/
│   │   ├── staging/                  # sources.yml + stg_* views
│   │   ├── intermediate/             # int_invoices_deduplicated view
│   │   └── marts/                    # fct_emissions_ledger table
│   └── tests/                        # Singular data quality tests
├── data/
│   ├── mock_invoices/                # Invoice CSVs, one per office
│   ├── emission_factors.csv
│   └── bad_data/corrupt_invoice.csv  # Fixture for the failure simulation
├── docker/
│   └── init_db.sql                   # Runs once on an empty local Postgres volume
├── docker-compose.yml
├── Dockerfile                        # Airflow image with extra Python packages and a dbt venv
├── requirements.txt                  # Scripts, dbt and dashboard packages
├── .streamlit/
│   └── secrets.toml.example          # Optional explicit data sources for the dashboard
└── .env.example
```

## What the dashboard shows

- **KPIs:** total t CO₂e, Scope 1 and Scope 2 with their share of the total, and energy cost per currency.
- **Emissions by office and scope:** stacked bars per office.
- **Monthly emissions trend:** an area chart per scope, with a legend explaining each scope.
- **Electricity grid intensity:** kg CO₂e per kWh per office, from Paris (0.035) to Frankfurt (0.353), with the factor source on hover.
- **Ledger audit:** invoice-level detail with consumption, unit, factor, factor source and CO₂e, plus a CSV export.

Filters: reporting year, office and GHG scope.

## FAQ

### Why synthetic invoices?

Real utility invoices are private. The generator produces data with the properties that make carbon accounting hard: mixed units and currencies, seasonal consumption, and a re-issued correction to deduplicate. It's seeded, so anyone can reproduce the same ledger.

### Why is `raw` append-only instead of truncated and reloaded?

An emissions ledger has to be auditable: it must be possible to show what was received and when. Every run appends rows with their `source_file` and `ingestion_timestamp`, and database triggers reject `UPDATE`, `DELETE` and `TRUNCATE` even for the `postgres` user. Corrections and re-runs are resolved in dbt (`int_invoices_deduplicated`, `stg_emission_factors`), not by rewriting history.

### Why is every column in `raw` TEXT?

The raw layer is an exact copy of the source, so it should never reject a row. If a column were typed and a file had an empty string or a bad value, `COPY` would fail the whole load. Casting and validation happen in dbt, where problems are visible and tested.

### How are the emission factors chosen?

The script asks Climatiq's Search endpoint for grid-mix electricity factors (Scope 2, electricity generation) and stationary diesel combustion factors (Scope 1), preferring each country's national agency: EPA, BEIS/DESNZ, UBA and ADEME. Search doesn't return factor values on standard plans, so each value comes from the Estimate endpoint for one unit of activity, already in the invoice's unit. Results are filtered to avoid well-to-tank (Scope 3) and vehicle (mobile) factors.

### Why does Ecuador use a proxy factor?

Climatiq has no grid factor for Ecuador, and IEA factors are licensed and can't be redistributed. The Quito row uses Ember's open data (via Our World in Data), labeled `IEA proxy` in `factor_source`. Ember is a lifecycle figure, slightly higher than a combustion-only Scope 2 factor. Ecuadorian diesel uses the EPA factor, and French diesel the UK one, both marked as proxies.

### Why isn't energy cost summed into one number?

Invoices are in USD, GBP and EUR, and the data has no exchange rates. Summing them would be meaningless, so the dashboard shows the total per currency. A single figure would need an FX table in the dbt model.

### Why location-based Scope 2?

The location-based method uses the average emission factor of each country's grid, which only needs consumption data. The market-based method would also need each office's electricity contracts and renewable energy certificates, which the invoices don't include.

### Why upload to MinIO first instead of loading the CSVs straight into Postgres?

It copies a common production pattern: land raw files in an object store (S3, GCS or similar), then load the warehouse from there. MinIO speaks the S3 API, so the same code would work against AWS S3.

### Why `pgsty/silo` instead of the official MinIO image?

MinIO stopped publishing community Docker images and deleted `minio/minio` and `minio/mc` from Docker Hub in 2026, and quay.io now requires login. `pgsty/silo` is an actively maintained fork and a drop-in replacement: same environment variables, same server command and health endpoint.

### Why does dbt have its own virtualenv in the Airflow image?

dbt's dependency pins clash with Airflow's constraints. A separate venv keeps both working. The base image sets `PIP_USER=true`, which pip rejects inside a venv, so the Dockerfile overrides it for that install.

### Why are the dbt schemas `staging`, `intermediate` and `marts`, not `public_staging`?

By default dbt prefixes a model's `+schema` with the profile's schema. The project overrides `generate_schema_name` so `+schema` is used exactly as written.

### Why are staging and intermediate views and marts a table?

Staging and intermediate are light transformations, cheap to keep as always-fresh views. The mart is what the dashboard queries, so it's a table. Some database clients list views apart from tables, or not at all, so look under **Views** if `staging` seems empty.

## Future evolution

- **Market-based Scope 2:** add supplier contracts and renewable energy certificates and report both methods, as the GHG Protocol recommends.
- **Regional grid factors:** use eGRID subregions in the US (NYC's subregion is about 0.39 kg CO₂e/kWh, above the national average).
- **FX conversion:** an exchange-rate seed or table to report total energy cost in one currency.
- **Idempotent landing:** skip files whose content was already ingested, and partition MinIO by load date (`landing/invoices/dt=YYYY-MM-DD/`) to keep every delivery.
- **Quarantine at ingestion:** reject or isolate rows with an empty `invoice_id` or negative consumption before they reach the append-only `raw` layer.
- **Scope 3:** business travel and purchased goods with Climatiq's travel and procurement endpoints.
