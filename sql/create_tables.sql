-- Reference DDL. The ETL creates these automatically on first run, so you do
-- NOT need to run this by hand. It's here so you can see exactly what's built.
-- Replace <PROJECT>, <DATASET> and <app> before running.

CREATE TABLE IF NOT EXISTS `<PROJECT>.<DATASET>.orders_<app>` (
  order_id     STRING NOT NULL,
  company      STRING,
  source       STRING,          -- 'original' or 'updated'
  time_stamp   STRING,          -- raw sale timestamp from sticky.io
  time_of_sale TIMESTAMP,       -- parsed to UTC
  date_of_sale DATE,            -- sale date in reporting timezone
  loaded_at    TIMESTAMP,
  raw_order    STRING           -- the WHOLE order as JSON text (nothing dropped)
)
PARTITION BY date_of_sale
CLUSTER BY order_id;

CREATE TABLE IF NOT EXISTS `<PROJECT>.<DATASET>._etl_state` (
  company              STRING NOT NULL,
  mode                 STRING,      -- 'backfill' / 'incremental'
  window_key           STRING,      -- e.g. '2025-11' or 'created' / 'updated'
  source               STRING,      -- 'original' / 'updated'
  status               STRING,      -- 'completed'
  orders_loaded        INT64,
  watermark_time_stamp STRING,
  updated_at           TIMESTAMP
);
