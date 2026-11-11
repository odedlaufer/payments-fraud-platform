{{ config(unique_key='customer_id') }}

-- Latest committed state of public.customers; deletes kept as is_deleted = true (FR-L1).
{{ current_state(ref('stg_cdc__customers'), 'customer_id') }}
