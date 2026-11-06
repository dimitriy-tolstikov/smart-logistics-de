DELETE FROM mart.route_card m
USING pg_temp.route_refresh_routes r
WHERE m.loading_city_gcid = r.loading_city_gcid
  AND m.unloading_city_gcid = r.unloading_city_gcid;
