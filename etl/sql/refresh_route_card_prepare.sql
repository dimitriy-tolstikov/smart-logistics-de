-- Params: start_date, end_date — both the stored and current loading_date windows
CREATE TEMP TABLE route_refresh_routes ON COMMIT DROP AS
SELECT
    c.loading_city_gcid,
    c.unloading_city_gcid
FROM core.fact_prices c
WHERE c.loading_date >= %(start_date)s::date
  AND c.loading_date < %(end_date)s::date
UNION
SELECT
    p.loading_city_gcid,
    p.unloading_city_gcid
FROM public.prices p
WHERE p.loading_date >= %(start_date)s::date
  AND p.loading_date < %(end_date)s::date;
