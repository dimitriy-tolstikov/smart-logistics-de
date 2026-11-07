CREATE INDEX IF NOT EXISTS prices_loading_date_idx ON public.prices (loading_date);

-- DISTINCT ON (gc_id) ORDER BY gc_id, id in the ML city dedup.
CREATE INDEX IF NOT EXISTS cities_gc_id_id_idx ON public.cities (gc_id, id);

-- Mart refresh looks up bodies and price_dates for a route-sized set of deals.
CREATE INDEX IF NOT EXISTS price_body_types_price_id_idx ON public.price_body_types (price_id);
CREATE INDEX IF NOT EXISTS price_dates_price_id_date_idx ON public.price_dates (price_id, price_date);
