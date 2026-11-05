-- Raw landing tables: every source column is stored as TEXT exactly as received,
-- plus audit columns. Typing and deduplication happen downstream (staging).
-- Applied once per warehouse by the DAG's run_migrations task (tracked in
-- public.schema_migrations); still idempotent, so re-applying it is harmless.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.invoices (
    invoice_id              TEXT,
    office_id               TEXT,
    country_code            TEXT,
    issue_date              TEXT,
    consumption_start_date  TEXT,
    consumption_end_date    TEXT,
    energy_type             TEXT,
    usage_amount            TEXT,
    usage_unit              TEXT,
    cost_amount             TEXT,
    currency                TEXT,
    source_file             TEXT,
    ingestion_timestamp     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.emission_factors (
    factor_id               TEXT,
    country_code            TEXT,
    energy_type             TEXT,
    activity_unit           TEXT,
    co2e_factor             TEXT,
    source_dataset          TEXT,
    factor_year             TEXT,
    retrieved_at            TEXT,
    source_file             TEXT,
    ingestion_timestamp     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Append-only guard: reject UPDATE, DELETE and TRUNCATE on raw tables.
CREATE OR REPLACE FUNCTION raw.reject_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'raw.% is append-only: % is not allowed', TG_TABLE_NAME, TG_OP;
END;
$$;

DO $$
DECLARE
    tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['invoices', 'emission_factors'] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I_no_row_mutation ON raw.%I', tbl, tbl);
        EXECUTE format('CREATE TRIGGER %I_no_row_mutation BEFORE UPDATE OR DELETE ON raw.%I '
                       'FOR EACH ROW EXECUTE FUNCTION raw.reject_mutation()', tbl, tbl);
        EXECUTE format('DROP TRIGGER IF EXISTS %I_no_truncate ON raw.%I', tbl, tbl);
        EXECUTE format('CREATE TRIGGER %I_no_truncate BEFORE TRUNCATE ON raw.%I '
                       'FOR EACH STATEMENT EXECUTE FUNCTION raw.reject_mutation()', tbl, tbl);
    END LOOP;
END;
$$;
