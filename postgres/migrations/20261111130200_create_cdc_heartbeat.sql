-- migrate:up

-- Debezium updates this row on an interval (heartbeat.action.query) so the
-- replication slot keeps advancing while the business tables are idle (FR-CDC3).
create table public.cdc_heartbeat (
    id smallint primary key check (id = 1),
    beat_at timestamptz not null default now()
);

insert into public.cdc_heartbeat (id) values (1);

-- migrate:down

drop table public.cdc_heartbeat;
