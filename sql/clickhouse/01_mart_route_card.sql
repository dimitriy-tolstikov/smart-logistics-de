-- Analytical route card on ClickHouse (bonus ★).
-- Same grain and metrics as PostgreSQL mart.route_card.
-- ReplacingMergeTree(computed_at): re-sync is idempotent; FINAL for exact reads.

CREATE DATABASE IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS mart.route_card
(
    month               Date,
    loading_city_gcid   Int32,
    unloading_city_gcid Int32,
    body_group_std_id   Int32,
    weight_category     LowCardinality(String),
    volume_category     Nullable(String),
    p25_trip            Nullable(Float64),
    median_trip         Nullable(Float64),
    p75_trip            Nullable(Float64),
    avg_trip            Nullable(Float64),
    p25_km              Nullable(Float64),
    median_km           Nullable(Float64),
    p75_km              Nullable(Float64),
    trips_count         UInt64,
    carriers_count      Nullable(UInt64),
    cv_rate             Nullable(Float64),
    mom_pct             Nullable(Float64),
    yoy_pct             Nullable(Float64),
    reliability_level   LowCardinality(String),
    as_of               Date,
    computed_at         DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (month, loading_city_gcid, unloading_city_gcid, body_group_std_id, weight_category)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS mart.ml_price_predictions
(
    price_id            Int64,
    loading_date        Date,
    loading_city_gcid   Int32,
    unloading_city_gcid Int32,
    body_group          String,
    actual_price_rub    Nullable(Float64),
    predicted_price_rub Float64,
    abs_error_rub       Nullable(Float64),
    ape_pct             Nullable(Float64),
    model_name          LowCardinality(String),
    scored_at           DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(scored_at)
ORDER BY price_id
SETTINGS index_granularity = 8192;
