with source as (

    select * from {{ source('raw', 'emission_factors') }}

),

typed as (

    select
        trim(factor_id)                             as factor_id,
        upper(trim(country_code))                   as country_code,
        lower(trim(energy_type))                    as energy_type,
        trim(activity_unit)                         as activity_unit,
        cast(co2e_factor as numeric(12, 6))         as co2e_factor,
        trim(source_dataset)                        as source_dataset,
        cast(factor_year as integer)                as factor_year,
        cast(retrieved_at as timestamptz)           as retrieved_at,
        source_file,
        ingestion_timestamp,
        -- raw is append-only: every DAG run re-lands the factors file, so keep only the
        -- latest factor per country/energy type to avoid fanning out the ledger join.
        row_number() over (
            partition by upper(trim(country_code)), lower(trim(energy_type))
            order by ingestion_timestamp desc, cast(retrieved_at as timestamptz) desc
        )                                           as rn
    from source

)

select
    factor_id,
    country_code,
    energy_type,
    activity_unit,
    co2e_factor,
    source_dataset,
    factor_year,
    retrieved_at,
    source_file,
    ingestion_timestamp
from typed
where rn = 1
