{{ config(unique_key='merchant_id') }}

-- Latest committed state of public.merchants; deletes kept as is_deleted = true (FR-L1).
{{ current_state(ref('stg_cdc__merchants'), 'merchant_id') }}
