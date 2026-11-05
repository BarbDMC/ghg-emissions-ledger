-- co2e = activity_amount * co2e_factor is only valid when both use the same unit
-- (e.g. liters of diesel against a kg CO2e / liter factor).
select ledger.invoice_id, ledger.activity_unit, factors.activity_unit as factor_unit
from {{ ref('fct_emissions_ledger') }} as ledger
inner join {{ ref('stg_emission_factors') }} as factors
    on ledger.country_code = factors.country_code
    and ledger.energy_type = factors.energy_type
where ledger.activity_unit <> factors.activity_unit
