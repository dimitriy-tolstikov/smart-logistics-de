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
