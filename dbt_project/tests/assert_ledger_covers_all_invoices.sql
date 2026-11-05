-- The ledger uses an inner join: any invoice without a matching emission factor
-- would silently disappear from the emissions total. Fail if that happens.
select invoices.invoice_id, invoices.country_code, invoices.energy_type
from {{ ref('int_invoices_deduplicated') }} as invoices
left join {{ ref('fct_emissions_ledger') }} as ledger
    on invoices.invoice_id = ledger.invoice_id
where ledger.invoice_id is null
