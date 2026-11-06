-- Params: start_date, end_date — both the stored and current loading_date windows
DELETE FROM dq.quarantine_prices q
USING public.prices p
WHERE q.price_id = p.id
  AND (
      (q.loading_date >= %(start_date)s::date AND q.loading_date < %(end_date)s::date)
      OR
      (p.loading_date >= %(start_date)s::date AND p.loading_date < %(end_date)s::date)
  );
