CREATE TABLE IF NOT EXISTS enriched_flights (
    id                    SERIAL PRIMARY KEY,
    fetched_at            TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Hop 1: OpenSky flight data
    icao24                VARCHAR(10),          -- unique aircraft transponder ID
    callsign              VARCHAR(20),
    origin_country        VARCHAR(100),
    latitude              NUMERIC(9, 6),
    longitude             NUMERIC(9, 6),
    altitude_m            NUMERIC(8, 2),        -- barometric altitude in metres
    velocity_ms           NUMERIC(7, 2),        -- ground speed m/s
    heading               NUMERIC(6, 2),        -- track angle in degrees
    vertical_rate         NUMERIC(7, 2),        -- climb/descent rate m/s
    on_ground             BOOLEAN,

    -- Hop 2: Weather at closest major airport
    nearest_airport       VARCHAR(10),
    origin_weather_temp   NUMERIC(5, 2),        -- °C
    origin_weather_cond   VARCHAR(50),
    origin_weather_wind   NUMERIC(6, 2),        -- m/s
    origin_weather_humid  SMALLINT,             -- %

    -- Hop 3: Lufthansa aircraft + airline enrichment
    aircraft_code         VARCHAR(10),
    aircraft_name         VARCHAR(100),
    airline_name          VARCHAR(100),

    -- Derived
    altitude_ft           NUMERIC(8, 2),        -- altitude_m * 3.28084
    velocity_kmh          NUMERIC(7, 2)         -- velocity_ms * 3.6
);

CREATE INDEX IF NOT EXISTS idx_flights_fetched
    ON enriched_flights (fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_flights_icao
    ON enriched_flights (icao24, fetched_at DESC);
