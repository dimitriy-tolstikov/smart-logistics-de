-- Params: start_date, end_date — loading_date window on public.prices
-- Builds one classified staging table reused by fact + quarantine inserts.
CREATE TEMP TABLE core_load_staging ON COMMIT DROP AS
WITH rates AS (
    SELECT num_code, setting_date, value
    FROM public.exchange_rates
),
city_ok AS (
    SELECT DISTINCT gc_id FROM public.cities
),
expanded AS (
    SELECT
        p.id AS price_id,
        p.created_at,
        p.loading_date,
        p.end_loading_date,
        p.loading_city_gcid,
        p.unloading_city_gcid,
        p.distance,
        p.total_weight,
        p.total_volume,
        p.currency_code,
        p.price AS price_original,
        p.price_type,
        p.is_fact,
        p.complex,
        p.outlier_mark,
        p.organization_id,
        p.subscriber_id,
        p.nearest_rate_date,
        p.body_type AS body_type_source,
        p.weight_category AS source_weight_category,
        p.volume_category AS source_volume_category,
        round(p.total_weight::numeric, 1)::double precision AS w1,
        CASE
            WHEN p.total_volume > 0 AND p.total_volume <= 120 THEN 'FTL'
            ELSE 'OTH'
        END AS volume_category_filled,
        CASE
            WHEN round(p.total_weight::numeric, 1) >= 30 THEN 'G20T'
            WHEN round(p.total_weight::numeric, 1) >= 10 THEN 'FTL'
            WHEN round(p.total_weight::numeric, 1) >= 5 THEN '10T'
            WHEN round(p.total_weight::numeric, 1) >= 2.5 THEN '5T'
            WHEN round(p.total_weight::numeric, 1) >= 0.5 THEN 'L5T'
            ELSE 'OTH'
        END AS weight_category_filled
    FROM public.prices p
    WHERE p.loading_date >= %(start_date)s::date
      AND p.loading_date < %(end_date)s::date
),
norm AS (
    SELECT
        e.*,
        CASE
            WHEN e.currency_code = '643' THEN e.price_original
            WHEN e.currency_code IN ('840', '978') THEN e.price_original * r.value
            ELSE NULL
        END AS price_rub_conv
    FROM expanded e
    LEFT JOIN LATERAL (
        SELECT value
        FROM rates r
        WHERE r.num_code = e.currency_code
          AND e.nearest_rate_date IS NOT NULL
          AND r.setting_date = e.nearest_rate_date
        LIMIT 1
    ) r ON TRUE
)
SELECT
    n.*,
    CASE
        WHEN n.price_original <= 0 THEN 'price_non_positive'
        -- RUB trip prices <= 1000 are source-validation anomalies (too low for a full trip)
        WHEN n.currency_code = '643' AND n.price_original > 1000 THEN 'rub_price_extreme'
        WHEN n.distance <= 0 THEN 'distance_non_positive'
        WHEN n.total_weight < 0 THEN 'weight_negative'
        WHEN n.total_volume < 0 THEN 'volume_negative'
        WHEN NOT EXISTS (
            SELECT 1 FROM city_ok c WHERE c.gc_id = n.loading_city_gcid
        ) OR NOT EXISTS (
            SELECT 1 FROM city_ok c2 WHERE c2.gc_id = n.unloading_city_gcid
        ) THEN 'city_unknown'
        WHEN n.currency_code IN ('840', '978') AND n.nearest_rate_date IS NULL THEN 'fx_date_missing'
        WHEN n.currency_code IN ('840', '978') AND n.price_rub_conv IS NULL THEN 'fx_rate_missing'
        WHEN n.currency_code NOT IN ('643', '840', '978') THEN 'currency_unsupported'
        ELSE 'ok'
    END AS dq_status
FROM norm n;
