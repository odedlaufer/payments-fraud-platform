-- SCD2 history of public.merchants: one row per version (FR-L2, ADR-006).
{{ scd2(ref('stg_cdc__merchants'), 'merchant_id') }}
