-- Params: as_of (date)
WITH params AS (
    SELECT (%(as_of)s::date) AS as_of
),
touched_prices AS (
    SELECT f.price_id
    FROM core.fact_prices f
    JOIN pg_temp.route_refresh_routes rr
      ON rr.loading_city_gcid = f.loading_city_gcid
     AND rr.unloading_city_gcid = f.unloading_city_gcid
),
deal_body AS (
    SELECT tp.price_id, g.body_group_std_id
    FROM touched_prices tp
    CROSS JOIN LATERAL core.price_body_groups(tp.price_id) g
),
base AS (
    SELECT DISTINCT
        date_trunc('month', pd.price_date)::date AS month,
        f.loading_city_gcid,
        f.unloading_city_gcid,
        db.body_group_std_id,
        f.weight_category,
        f.volume_category,
        f.price_id,
        f.price_rub,
        f.distance,
        f.organization_id,
        p.price_type,
        f.loading_date
    FROM core.fact_prices f
    JOIN pg_temp.route_refresh_routes rr
      ON rr.loading_city_gcid = f.loading_city_gcid
     AND rr.unloading_city_gcid = f.unloading_city_gcid
    JOIN public.prices p ON p.id = f.price_id
    JOIN public.price_dates pd ON pd.price_id = f.price_id
    JOIN deal_body db ON db.price_id = f.price_id
    CROSS JOIN params x
    WHERE f.is_fact
      AND NOT f.complex
      AND f.price_rub > 0
      AND f.distance > 0
      AND pd.price_date <= x.as_of
      AND COALESCE(f.outlier_mark, false) IS NOT TRUE
      AND p.is_fact
      AND NOT p.complex
),
agg AS (
    SELECT
        month,
        loading_city_gcid,
        unloading_city_gcid,
        body_group_std_id,
        weight_category,
        mode() WITHIN GROUP (ORDER BY volume_category) AS volume_category,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub) AS p25_trip,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY price_rub) AS median_trip,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub) AS p75_trip,
        avg(price_rub) AS avg_trip,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY price_rub / NULLIF(distance, 0)) AS p25_km,
        percentile_cont(0.50) WITHIN GROUP (ORDER BY price_rub / NULLIF(distance, 0)) AS median_km,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY price_rub / NULLIF(distance, 0)) AS p75_km,
        count(DISTINCT price_id)::bigint AS trips_count,
        count(DISTINCT organization_id) FILTER (WHERE price_type IN (3, 9) AND organization_id IS NOT NULL) AS carriers_count,
        CASE
            WHEN count(*) >= 2 THEN stddev_samp(price_rub) / NULLIF(avg(price_rub), 0)
            ELSE NULL
        END AS cv_rate,
        max(loading_date) AS last_loading_date
    FROM base
    GROUP BY 1, 2, 3, 4, 5
),
enriched AS (
    SELECT
        a.*,
        CASE
            WHEN p.as_of >= a.last_loading_date
                AND (p.as_of - a.last_loading_date) <= 30 THEN true
            ELSE false
        END AS is_recent_window,
        prev.median_trip AS prev_median,
        yoy.median_trip AS yoy_median
    FROM agg a
    CROSS JOIN params p
    LEFT JOIN agg prev
      ON prev.loading_city_gcid = a.loading_city_gcid
     AND prev.unloading_city_gcid = a.unloading_city_gcid
     AND prev.body_group_std_id = a.body_group_std_id
     AND prev.weight_category = a.weight_category
     AND prev.month = (a.month - INTERVAL '1 month')::date
    LEFT JOIN agg yoy
      ON yoy.loading_city_gcid = a.loading_city_gcid
     AND yoy.unloading_city_gcid = a.unloading_city_gcid
     AND yoy.body_group_std_id = a.body_group_std_id
     AND yoy.weight_category = a.weight_category
     AND yoy.month = (a.month - INTERVAL '1 year')::date
)
INSERT INTO mart.route_card (
    month,
    loading_city_gcid,
    unloading_city_gcid,
    body_group_std_id,
    weight_category,
    volume_category,
    p25_trip,
    median_trip,
    p75_trip,
    avg_trip,
    p25_km,
    median_km,
    p75_km,
    trips_count,
    carriers_count,
    cv_rate,
    mom_pct,
    yoy_pct,
    reliability_level,
    as_of,
    computed_at
)
SELECT
    e.month,
    e.loading_city_gcid,
    e.unloading_city_gcid,
    e.body_group_std_id,
    e.weight_category,
    e.volume_category,
    e.p25_trip,
    e.median_trip,
    e.p75_trip,
    e.avg_trip,
    e.p25_km,
    e.median_km,
    e.p75_km,
    e.trips_count,
    e.carriers_count,
    e.cv_rate,
    mart.change_pct(e.median_trip, e.prev_median) AS mom_pct,
    mart.change_pct(e.median_trip, e.yoy_median) AS yoy_pct,
    CASE
        WHEN e.trips_count >= 30
             AND COALESCE(e.carriers_count, 0) >= 5
             AND e.is_recent_window THEN 'HIGH'
        WHEN e.trips_count >= 10 THEN 'MEDIUM'
        ELSE 'LOW_DATA'
    END AS reliability_level,
    p.as_of,
    now()
FROM enriched e
CROSS JOIN params p;
