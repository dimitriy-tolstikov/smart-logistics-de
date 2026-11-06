-- Params: start_date, end_date — both the stored and current loading_date windows
DELETE FROM core.fact_prices c
USING public.prices p
WHERE c.price_id = p.id
  AND (
      (c.loading_date >= %(start_date)s::date AND c.loading_date < %(end_date)s::date)
      OR
      (p.loading_date >= %(start_date)s::date AND p.loading_date < %(end_date)s::date)
  );
