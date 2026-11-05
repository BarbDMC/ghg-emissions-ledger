with invoices as (

    select * from {{ ref('int_invoices_deduplicated') }}

),

factors as (

    select * from {{ ref('stg_emission_factors') }}

)

select
    md5(invoices.invoice_id)                                        as ledger_id,
    invoices.invoice_id,
    invoices.office_id,
    invoices.country_code,
    invoices.issue_date,
    cast(extract(year from invoices.consumption_start_date) as integer)  as reporting_year,
    cast(extract(month from invoices.consumption_start_date) as integer) as reporting_month,
    invoices.energy_type,
    case
        when invoices.energy_type = 'diesel' then 'Scope 1'
        when invoices.energy_type = 'electricity' then 'Scope 2'
    end                                                             as ghg_scope,
    invoices.usage_amount                                           as activity_amount,
    invoices.usage_unit                                             as activity_unit,
    factors.co2e_factor,
    factors.source_dataset                                          as factor_source,
    factors.factor_year,
    round(invoices.usage_amount * factors.co2e_factor, 4)           as co2e_kg,
    round((invoices.usage_amount * factors.co2e_factor) / 1000.0, 4) as co2e_tonnes,
    invoices.cost_amount,
    invoices.currency
from invoices
inner join factors
    on invoices.country_code = factors.country_code
    and invoices.energy_type = factors.energy_type
