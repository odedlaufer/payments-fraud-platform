-- migrate:up

-- Simulated time of the latest status change (pending at creation, then approved,
-- declined or reversed). event_ts stays the authorization time; this records when a
-- reversal happened, which merchant_refund_rate_7d needs point in time (ADR-003).
alter table public.transactions add column status_event_ts timestamptz;

alter table public.transactions add constraint transactions_status_after_authorization check (
    status_event_ts is null or status_event_ts >= event_ts
);

-- migrate:down

alter table public.transactions drop constraint transactions_status_after_authorization;
alter table public.transactions drop column status_event_ts;
