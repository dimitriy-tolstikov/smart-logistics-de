-- Params: start_date, end_date — both the stored and current loading_date windows.
-- Captured before DELETE. A row whose new loading_date falls inside the window is
-- removed from core even when the stored date is outside it, so the stored city pair
-- has to be refreshed too. Otherwise a city change leaves the old route card in place.
CREATE TEMP TABLE route_refresh_routes ON COMMIT DROP AS
SELECT
    c.loading_city_gcid,
    c.unloading_city_gcid
FROM core.fact_prices c
JOIN public.prices p ON p.id = c.price_id
WHERE (
        c.loading_date >= %(start_date)s::date
        AND c.loading_date < %(end_date)s::date
      )
   OR (
        p.loading_date >= %(start_date)s::date
        AND p.loading_date < %(end_date)s::date
      )
UNION
SELECT
    p.loading_city_gcid,
    p.unloading_city_gcid
FROM public.prices p
WHERE p.loading_date >= %(start_date)s::date
  AND p.loading_date < %(end_date)s::date;
