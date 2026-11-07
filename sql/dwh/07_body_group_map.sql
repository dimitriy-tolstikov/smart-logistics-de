-- Shared body-group mapping used by mart refresh and ML feature load.
CREATE OR REPLACE FUNCTION core.map_body_group_std_id(std_id integer)
RETURNS integer
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN std_id IN (56, 57, 58, 59, 60, 61) THEN 56
        WHEN std_id IN (4, 46, 47, 48) THEN 4
        WHEN std_id = 2 THEN 2
        WHEN std_id IN (1, 15) THEN 15
        WHEN std_id IS NULL THEN -1
        ELSE -2
    END;
$$;

-- Groups for a single deal: link rows, or prices.body_type when the link table is empty.
CREATE OR REPLACE FUNCTION core.price_body_groups(p_price_id bigint)
RETURNS TABLE (body_group_std_id integer)
LANGUAGE sql
STABLE
AS $$
    SELECT DISTINCT core.map_body_group_std_id(bt.std_id) AS body_group_std_id
    FROM public.price_body_types pbt
    JOIN public.body_types bt ON bt.id = pbt.body_type_id
    WHERE pbt.price_id = p_price_id
    UNION
    SELECT core.map_body_group_std_id(bt.std_id)
    FROM core.fact_prices f
    LEFT JOIN public.body_types bt ON bt.id = f.body_type_source
    WHERE f.price_id = p_price_id
      AND NOT EXISTS (
          SELECT 1 FROM public.price_body_types pbt WHERE pbt.price_id = p_price_id
      );
$$;

CREATE OR REPLACE VIEW core.price_body_group AS
SELECT f.price_id, g.body_group_std_id
FROM core.fact_prices f
CROSS JOIN LATERAL core.price_body_groups(f.price_id) g;

-- MoM/YoY: missing calendar cell and a zero baseline are both NULL, not a fake 0%.
CREATE OR REPLACE FUNCTION mart.change_pct(curr double precision, prev double precision)
RETURNS double precision
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE
        WHEN prev IS NULL OR prev = 0 THEN NULL
        ELSE (curr - prev) / prev * 100.0
    END;
$$;
