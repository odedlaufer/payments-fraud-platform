-- migrate:up

-- Debezium signaling table: inserting an execute-snapshot row asks the connector for an
-- incremental snapshot, re-emitting current rows as 'r' events while streaming continues.
-- Debezium also writes its snapshot window watermarks here, hence insert for its role.
create table public.debezium_signal (
    id varchar(42) primary key,
    type varchar(32) not null,
    data varchar(2048)
);

grant select, insert on public.debezium_signal to debezium;

alter publication payments_cdc add table public.debezium_signal;

-- migrate:down

alter publication payments_cdc drop table public.debezium_signal;
drop table public.debezium_signal;
