-- Core DWH: cleaned, currency-normalized deal grain (one row per prices.id)
CREATE TABLE IF NOT EXISTS core.fact_prices (
    price_id            bigint PRIMARY KEY REFERENCES public.prices (id) ON DELETE CASCADE,
    created_at          timestamp(0),
    loading_date        date NOT NULL,
    end_loading_date    date,
    loading_city_gcid   integer NOT NULL,
    unloading_city_gcid integer NOT NULL,
    distance            double precision NOT NULL,
    total_weight        double precision NOT NULL,
    total_volume        double precision NOT NULL,
    currency_code       varchar(3) NOT NULL,
    price_original      double precision NOT NULL,
    price_rub           double precision NOT NULL,
    nearest_rate_date   date,
    price_type          smallint NOT NULL,
    is_fact             boolean NOT NULL,
    complex             boolean NOT NULL,
    outlier_mark        boolean NOT NULL,
    organization_id     integer,
    subscriber_id       integer,
    weight_category     varchar(4) NOT NULL,
    volume_category     varchar(4) NOT NULL,
    body_type_source    integer,
    source_row_hash     text NOT NULL,
    loaded_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS core_fact_prices_loading_date_idx ON core.fact_prices (loading_date);
CREATE INDEX IF NOT EXISTS core_fact_prices_route_idx
    ON core.fact_prices (loading_city_gcid, unloading_city_gcid);
