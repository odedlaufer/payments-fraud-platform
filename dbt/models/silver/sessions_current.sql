{{ config(unique_key='session_id') }}

-- Latest committed state of public.sessions; deletes kept as is_deleted = true (FR-L1).
{{ current_state(ref('stg_cdc__sessions'), 'session_id') }}
