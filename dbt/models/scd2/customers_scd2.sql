-- SCD2 history of public.customers: one row per version (FR-L2, ADR-006).
{{ scd2(ref('stg_cdc__customers'), 'customer_id') }}
