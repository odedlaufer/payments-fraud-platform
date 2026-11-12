-- migrate:up

-- FR-SIM7 / DC-4: a nullable column added while the platform runs. Online
-- transactions carry the risk score of the session's device (higher for a device
-- new to the customer); card-present transactions leave it null.
alter table public.transactions add column device_risk_score numeric(5, 4);

alter table public.transactions add constraint transactions_device_risk_score_range check (
    device_risk_score is null or device_risk_score between 0 and 1
);

-- migrate:down

alter table public.transactions drop constraint transactions_device_risk_score_range;
alter table public.transactions drop column device_risk_score;
