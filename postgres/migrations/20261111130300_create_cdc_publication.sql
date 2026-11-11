-- migrate:up

-- Full before-images on update and delete, so bronze keeps every prior value.
alter table public.customers replica identity full;
alter table public.cards replica identity full;
alter table public.merchants replica identity full;
alter table public.sessions replica identity full;
alter table public.transactions replica identity full;
alter table public.chargebacks replica identity full;

-- The six domain tables plus the heartbeat (FR-CDC1, FR-CDC3). sim.sim_truth
-- is deliberately excluded.
create publication payments_cdc for table
public.customers,
public.cards,
public.merchants,
public.sessions,
public.transactions,
public.chargebacks,
public.cdc_heartbeat;

-- migrate:down

drop publication payments_cdc;

alter table public.customers replica identity default;
alter table public.cards replica identity default;
alter table public.merchants replica identity default;
alter table public.sessions replica identity default;
alter table public.transactions replica identity default;
alter table public.chargebacks replica identity default;
