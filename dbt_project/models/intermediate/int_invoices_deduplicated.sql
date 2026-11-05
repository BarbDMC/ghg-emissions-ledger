-- Keep only the latest valid version of each invoice. Corrected invoices are
-- re-issued with the same invoice_id and a later issue_date; re-runs of the
-- ingestion DAG re-land the same rows with a later ingestion_timestamp.

with ranked as (

    select
        *,
        row_number() over (
            partition by invoice_id
            order by ingestion_timestamp desc, issue_date desc
        ) as rn
    from {{ ref('stg_invoices') }}

)

select
    invoice_id,
    office_id,
    country_code,
    issue_date,
    consumption_start_date,
    consumption_end_date,
    energy_type,
    usage_amount,
    usage_unit,
    cost_amount,
    currency,
    source_file,
    ingestion_timestamp
from ranked
where rn = 1
