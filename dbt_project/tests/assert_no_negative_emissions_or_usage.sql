-- Consumption and emissions can never be negative; any row returned is a data error.
select
    ledger_id,
    invoice_id,
    office_id,
    energy_type,
    activity_amount,
    activity_unit,
    co2e_kg,
    co2e_tonnes
from {{ ref('fct_emissions_ledger') }}
where activity_amount < 0
   or co2e_kg < 0
   or co2e_tonnes < 0
