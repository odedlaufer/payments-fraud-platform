{{ config(unique_key='transaction_id') }}

-- Latest committed state of public.transactions; deletes kept as is_deleted = true (FR-L1).
{{ current_state(ref('stg_cdc__transactions'), 'transaction_id') }}
