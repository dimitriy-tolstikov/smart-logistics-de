-- Idempotent upgrade for existing deployments created from older 05_etl_meta.sql.
ALTER TABLE etl_meta.runs
    ADD COLUMN IF NOT EXISTS as_of date;

ALTER TABLE etl_meta.runs
    ADD COLUMN IF NOT EXISTS status text;

ALTER TABLE etl_meta.runs
    ADD COLUMN IF NOT EXISTS error_message text;

UPDATE etl_meta.runs
SET status = COALESCE(status, CASE WHEN finished_at IS NULL THEN 'running' ELSE 'success' END)
WHERE status IS NULL;

ALTER TABLE etl_meta.runs
    ALTER COLUMN status SET DEFAULT 'running';

ALTER TABLE etl_meta.runs
    ALTER COLUMN status SET NOT NULL;
