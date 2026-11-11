-- migrate:up

-- Ground truth for evaluation only (PRD section 3). Lives in its own schema
-- and is never added to the CDC publication, so it cannot leak into features.
create schema sim;

create table sim.sim_truth (
    transaction_id bigint primary key references public.transactions (transaction_id),
    is_fraud boolean not null,
    pattern text check (pattern in (
        'card_testing',
        'account_takeover',
        'impossible_travel',
        'merchant_bust_out',
        'friendly_fraud'
    )),
    created_at timestamptz not null default now(),
    -- Friendly fraud is a legitimate transaction with a chargeback (label noise).
    -- CASE, not OR: a check that evaluates to NULL would accept the row.
    constraint pattern_matches_label check (
        case
            when pattern is null then not is_fraud
            when pattern = 'friendly_fraud' then not is_fraud
            else is_fraud
        end
    )
);

-- migrate:down

drop table sim.sim_truth;
drop schema sim;
