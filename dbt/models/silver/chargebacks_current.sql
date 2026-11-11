{{ config(unique_key='chargeback_id') }}

-- Latest committed state of public.chargebacks; deletes kept as is_deleted = true (FR-L1).
{{ current_state(ref('stg_cdc__chargebacks'), 'chargeback_id') }}
