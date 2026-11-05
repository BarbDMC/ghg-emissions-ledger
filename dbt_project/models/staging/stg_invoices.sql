with source as (

    select * from {{ source('raw', 'invoices') }}

),

typed as (

    select
        trim(invoice_id)                            as invoice_id,
        trim(office_id)                             as office_id,
        upper(trim(country_code))                   as country_code,
        cast(issue_date as date)                    as issue_date,
        cast(consumption_start_date as date)        as consumption_start_date,
        cast(consumption_end_date as date)          as consumption_end_date,
        lower(trim(energy_type))                    as energy_type,
        cast(usage_amount as numeric)               as usage_amount_raw,
        lower(trim(usage_unit))                     as usage_unit_raw,
        cast(cost_amount as numeric)                as cost_amount,
        upper(trim(currency))                       as currency,
        source_file,
        ingestion_timestamp
    from source

)

select
    invoice_id,
    office_id,
    country_code,
    issue_date,
    consumption_start_date,
    consumption_end_date,
    energy_type,
    case
        when usage_unit_raw = 'mwh' then usage_amount_raw * 1000
        else usage_amount_raw
    end                                             as usage_amount,
    case
        when usage_unit_raw in ('gal', 'gallon', 'gallons')        then 'gallon'
        when usage_unit_raw in ('l', 'liter', 'liters', 'litre', 'litres') then 'liter'
        when usage_unit_raw in ('kwh', 'mwh')                      then 'kWh'
    end                                             as usage_unit,
    usage_amount_raw,
    usage_unit_raw,
    cost_amount,
    currency,
    source_file,
    ingestion_timestamp
from typed
