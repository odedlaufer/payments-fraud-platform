{{ config(unique_key='card_id') }}

-- Latest committed state of public.cards; deletes kept as is_deleted = true (FR-L1).
{{ current_state(ref('stg_cdc__cards'), 'card_id') }}
