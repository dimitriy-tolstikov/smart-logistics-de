-- Quarantine for failed validation / FX lookup, replaced per ETL window
CREATE TABLE IF NOT EXISTS dq.quarantine_prices (
    id              bigserial PRIMARY KEY,
    price_id        bigint NOT NULL,
    reason_code     text NOT NULL,
    detail          jsonb,
    loading_date    date,
    captured_at     timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE dq.quarantine_prices ADD COLUMN IF NOT EXISTS loading_date date;
UPDATE dq.quarantine_prices q
SET loading_date = p.loading_date
FROM public.prices p
WHERE p.id = q.price_id AND q.loading_date IS NULL;

CREATE INDEX IF NOT EXISTS dq_quarantine_price_id_idx ON dq.quarantine_prices (price_id);
CREATE INDEX IF NOT EXISTS dq_quarantine_reason_idx ON dq.quarantine_prices (reason_code);
CREATE INDEX IF NOT EXISTS dq_quarantine_loading_date_idx ON dq.quarantine_prices (loading_date);
