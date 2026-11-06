-- Params: start_date, end_date (window already materialised in core_load_staging)
INSERT INTO dq.quarantine_prices (price_id, reason_code, detail, loading_date)
SELECT
    c.price_id,
    c.dq_status,
    jsonb_build_object(
        'currency_code', c.currency_code,
        'price', c.price_original,
        'distance', c.distance,
        'total_weight', c.total_weight,
        'total_volume', c.total_volume,
        'nearest_rate_date', c.nearest_rate_date
    ),
    c.loading_date
FROM core_load_staging c
WHERE c.dq_status <> 'ok'
   OR (c.dq_status = 'ok' AND c.price_rub_conv IS NULL);
