
-- DB for our data (timescaleDB)
-- CREATE DATABASE tsdb;

-- Make sure correct schema is in use, in tsdb database
CREATE SCHEMA IF NOT EXISTS data;
SET SEARCH_PATH = data;

-- ENTSOE tables
CREATE TABLE total_load_actual (
  timestamp TIMESTAMP PRIMARY KEY,
  actual_load NUMERIC
);

CREATE TABLE total_load_day_ahead (
  timestamp TIMESTAMP PRIMARY KEY,
  total_load NUMERIC
);

CREATE TABLE total_load_week_ahead (
  timestamp TIMESTAMP PRIMARY KEY,
  min_total_load NUMERIC,
  max_total_load NUMERIC
);

CREATE TABLE total_load_month_ahead (
  timestamp TIMESTAMP PRIMARY KEY,
  min_total_load NUMERIC,
  max_total_load NUMERIC
);

CREATE TABLE total_load_year_ahead (
  timestamp TIMESTAMP PRIMARY KEY,
  min_total_load NUMERIC,
  max_total_load NUMERIC
);

CREATE TABLE generation_forecast_day_ahead (
  timestamp TIMESTAMP PRIMARY KEY,
  scheduled_generation NUMERIC
);

CREATE TABLE generation_forecast_windsolar (
  timestamp TIMESTAMP PRIMARY KEY,
  solar_dayahead NUMERIC,
  solar_intraday NUMERIC,
  wind_dayahead NUMERIC,
  wind_intraday NUMERIC
);

CREATE TABLE actual_generation_per_type (
  timestamp TIMESTAMP PRIMARY KEY,
  fossil_brown_coal_lignite NUMERIC,
  fossil_gas NUMERIC,
  fossil_oil NUMERIC,
  hydro_pumped_storage NUMERIC,
  hydro_water_reservoir NUMERIC,
  solar NUMERIC,
  wind_onshore NUMERIC
);

CREATE TABLE day_ahead_prices (
  timestamp TIMESTAMP PRIMARY KEY,
  price NUMERIC
);

CREATE TABLE crossborder_flows (
  timestamp TIMESTAMP PRIMARY KEY,
  gr_al NUMERIC,
  al_gr NUMERIC,
  gr_bg NUMERIC,
  bg_gr NUMERIC,
  gr_it NUMERIC,
  it_gr NUMERIC,
  gr_mk NUMERIC,
  mk_gr NUMERIC,
  gr_tr NUMERIC,
  tr_gr NUMERIC
);

CREATE TABLE hydro_reservoir_storage (
  timestamp TIMESTAMP PRIMARY KEY,
  stored_energy NUMERIC
);

CREATE TABLE actual_generation_per_generation_unit (
  timestamp TIMESTAMP,
  point_id TEXT,
  value NUMERIC,
  PRIMARY KEY (timestamp, point_id)
);

-- IPTO tables
CREATE TABLE ipto_1day_ahead_load_forecast (
  timestamp TIMESTAMP PRIMARY KEY,
  load_forecast NUMERIC
);

CREATE TABLE ipto_2day_ahead_load_forecast (
  timestamp TIMESTAMP PRIMARY KEY,
  load_forecast NUMERIC
);

CREATE TABLE ipto_3intraday_load_forecast (
  timestamp TIMESTAMP PRIMARY KEY,
  load_forecast NUMERIC
);

CREATE TABLE ipto_1day_ahead_res_forecast (
  timestamp TIMESTAMP PRIMARY KEY,
  res_forecast NUMERIC
);

CREATE TABLE ipto_2day_ahead_res_forecast (
  timestamp TIMESTAMP PRIMARY KEY,
  res_forecast NUMERIC
);

CREATE TABLE ipto_3intraday_res_forecast (
  timestamp TIMESTAMP PRIMARY KEY,
  res_forecast NUMERIC
);

CREATE TABLE ipto_week_ahead_load_forecast (
  timestamp TIMESTAMP,
  target_timestamp TIMESTAMP,
  load_forecast NUMERIC,
  PRIMARY KEY (timestamp, target_timestamp)
);

CREATE TABLE ipto_net_interconnection_flows (
  timestamp TIMESTAMP PRIMARY KEY,
  albania NUMERIC,
  fyrom NUMERIC,
  bulgaria NUMERIC,
  turkey NUMERIC,
  italy NUMERIC
);

CREATE TABLE ipto_res_injections (
  timestamp TIMESTAMP PRIMARY KEY,
  res_injections NUMERIC
);

CREATE TABLE ipto_unit_production_and_system_facts (
  timestamp TIMESTAMP PRIMARY KEY,
  point_id TEXT,
  value NUMERIC
);

CREATE TABLE ipto_daily_energy_balance (
  timestamp TIMESTAMP PRIMARY KEY,
  lignite NUMERIC,
  natural_gas NUMERIC,
  hydroelectric NUMERIC,
  renewables NUMERIC,
  net_imports NUMERIC
);

-- ENTSO-G tables
CREATE TABLE entsog_flows_daily (
    timestamp TIMESTAMP,
    value NUMERIC,
    point_id TEXT,
    point_type TEXT,
    PRIMARY KEY (timestamp, point_id, point_type)
);

CREATE TABLE entsog_nominations_daily (
    timestamp TIMESTAMP,
    value NUMERIC,
    point_id TEXT,
    point_type TEXT,
    PRIMARY KEY (timestamp, point_id, point_type)
);

CREATE TABLE entsog_allocations_daily (
    timestamp TIMESTAMP,
    value NUMERIC,
    point_id TEXT,
    point_type TEXT,
    PRIMARY KEY (timestamp, point_id, point_type)
);

CREATE TABLE entsog_renominations_daily (
    timestamp TIMESTAMP,
    value NUMERIC,
    point_id TEXT,
    point_type TEXT,
    PRIMARY KEY (timestamp, point_id, point_type)
);

-- DESFA tables
CREATE TABLE desfa_flows_daily (
    timestamp TIMESTAMP,
    value NUMERIC,
    point_id TEXT,
    point_type TEXT,
    PRIMARY KEY (timestamp, point_id, point_type)
);

CREATE TABLE desfa_flows_hourly (
    timestamp TIMESTAMP PRIMARY KEY,
    value NUMERIC,
    point_id TEXT,
    point_type TEXT
);

CREATE TABLE desfa_ng_quality_yearly (
    timestamp TIMESTAMP,
    point_id TEXT,
    c1 NUMERIC,
    c2 NUMERIC,
    c3 NUMERIC,
    i_c4 NUMERIC,
    n_c4 NUMERIC,
    i_c5 NUMERIC,
    n_c5 NUMERIC,
    neo_c5 NUMERIC,
    c6_plus NUMERIC,
    n2 NUMERIC,
    co2 NUMERIC,
    gross_heating_value NUMERIC,
    wobbe_index NUMERIC,
    water_dew_point NUMERIC,
    hydrocarbon_dew_point_max NUMERIC,
    PRIMARY KEY (timestamp, point_id)
);

CREATE TABLE desfa_ng_pressure_monthly (
    timestamp TIMESTAMP,
    point_id TEXT,
    min_delivery_pressure NUMERIC,
    max_delivery_pressure NUMERIC,
    PRIMARY KEY (timestamp, point_id)
);

CREATE TABLE desfa_ng_gcv_daily (
    timestamp TIMESTAMP,
    value NUMERIC,
    point_id TEXT,
    point_type TEXT,
    PRIMARY KEY (timestamp, point_id, point_type)
);

CREATE TABLE desfa_nominations_daily (
    timestamp TIMESTAMP PRIMARY KEY,
    value NUMERIC,
    point_id TEXT,
    point_type TEXT
);

-- for postgrest, taken from https://postgrest.org/en/stable/tutorials/tut0.html#step-4-create-database-for-api
CREATE ROLE web_anon NOLOGIN;
GRANT USAGE ON SCHEMA data TO web_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA data TO web_anon;
CREATE ROLE authenticator NOINHERIT LOGIN PASSWORD 'postgres';
GRANT web_anon TO authenticator;

-- DB for dagster's records
CREATE DATABASE dagster_storage;
