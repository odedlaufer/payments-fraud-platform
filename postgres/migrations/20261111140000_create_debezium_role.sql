-- migrate:up

-- Least-privilege login for Debezium (FR-CDC2): replication plus read access
-- to the published tables only. No access to the sim schema. The password is
-- set outside migrations by postgres/roles.psql, from DEBEZIUM_DB_PASSWORD.
create role debezium with login replication;

do $$
begin
    execute format('grant connect on database %I to debezium', current_database());
end;
$$;

grant usage on schema public to debezium;
grant select on
public.customers,
public.cards,
public.merchants,
public.sessions,
public.transactions,
public.chargebacks,
public.cdc_heartbeat
to debezium;

-- Debezium runs heartbeat.action.query against this table (FR-CDC3).
grant update on public.cdc_heartbeat to debezium;

-- migrate:down

drop owned by debezium;
drop role debezium;
