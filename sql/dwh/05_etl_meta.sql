CREATE TABLE IF NOT EXISTS etl_meta.runs (
    id            bigserial PRIMARY KEY,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    start_date    date NOT NULL,
    end_date      date NOT NULL,
    as_of         date,
    status        text NOT NULL DEFAULT 'running',
    error_message text,
    stats         jsonb
);
