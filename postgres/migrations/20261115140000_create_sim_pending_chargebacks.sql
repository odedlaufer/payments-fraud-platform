-- migrate:up

-- Chargebacks the simulator has decided will be filed later (FR-SIM4: 1 to 60 simulated
-- days after the transaction). Kept in the database so they survive between simulator
-- runs; sim.* is never published, so CDC only ever sees the filed public.chargebacks row.
create table sim.pending_chargebacks (
    transaction_id bigint primary key references public.transactions (transaction_id),
    reason_code text not null,
    file_at timestamptz not null,
    created_at timestamptz not null default now()
);

create index pending_chargebacks_file_at on sim.pending_chargebacks (file_at);

-- migrate:down

drop table sim.pending_chargebacks;
