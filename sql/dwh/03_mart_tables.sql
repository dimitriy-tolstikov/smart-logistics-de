-- Route card / market corridor mart
-- Grain: month × loading city × unloading city × body group × weight category
-- volume_category is an attribute (mode within the cell), not part of the grain.
CREATE TABLE IF NOT EXISTS mart.route_card (
    month                  date NOT NULL,
    loading_city_gcid    integer NOT NULL,
    unloading_city_gcid  integer NOT NULL,
    body_group_std_id    integer NOT NULL,
    weight_category      varchar(4) NOT NULL,
    volume_category      varchar(4),
    p25_trip             double precision,
    median_trip          double precision,
    p75_trip             double precision,
    avg_trip             double precision,
    p25_km               double precision,
    median_km            double precision,
    p75_km               double precision,
    trips_count          bigint NOT NULL,
    carriers_count       bigint,
    cv_rate              double precision,
    mom_pct              double precision,
    yoy_pct              double precision,
    reliability_level    text NOT NULL,
    as_of                date NOT NULL,
    computed_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (month, loading_city_gcid, unloading_city_gcid, body_group_std_id, weight_category)
);

CREATE INDEX IF NOT EXISTS mart_route_card_as_of_idx ON mart.route_card (as_of);

-- ML batch scoring output
CREATE TABLE IF NOT EXISTS mart.ml_price_predictions (
    price_id              bigint PRIMARY KEY,
    loading_date          date NOT NULL,
    loading_city_gcid     integer NOT NULL,
    unloading_city_gcid   integer NOT NULL,
    body_group            text NOT NULL,
    weight_category       varchar(4),
    body_group_std_id     integer,
    actual_price_rub      double precision,
    predicted_price_rub   double precision NOT NULL,
    abs_error_rub         double precision,
    ape_pct               double precision,
    model_name            text NOT NULL,
    scored_at             timestamptz NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'mart'
          AND table_name = 'ml_price_predictions'
          AND column_name = 'body_group'
          AND data_type = 'integer'
    ) THEN
        ALTER TABLE mart.ml_price_predictions
            ALTER COLUMN body_group TYPE text USING body_group::text;
    END IF;
END $$;

ALTER TABLE mart.ml_price_predictions
    ADD COLUMN IF NOT EXISTS weight_category varchar(4);

ALTER TABLE mart.ml_price_predictions
    ADD COLUMN IF NOT EXISTS body_group_std_id integer;

CREATE INDEX IF NOT EXISTS mart_ml_pred_loading_date_idx ON mart.ml_price_predictions (loading_date);
CREATE INDEX IF NOT EXISTS mart_ml_pred_cell_idx
    ON mart.ml_price_predictions (loading_city_gcid, unloading_city_gcid, body_group_std_id, weight_category);
