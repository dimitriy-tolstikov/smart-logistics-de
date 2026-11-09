-- One row per batch scoring run. Metrics are the carrier holdout (price_type 3 and 9).
CREATE TABLE IF NOT EXISTS etl_meta.ml_runs (
    id              bigserial PRIMARY KEY,
    started_at      timestamptz NOT NULL DEFAULT now(),
    as_of           date NOT NULL,
    model_name      text NOT NULL,
    depth           integer NOT NULL,
    learning_rate   double precision NOT NULL,
    iterations      integer NOT NULL,
    tuned           boolean NOT NULL,
    train_rows      integer NOT NULL,
    test_rows       integer NOT NULL,
    r2              double precision,
    mape_pct        double precision,
    rmse            double precision,
    mae             double precision,
    scored_rows     integer NOT NULL
);
