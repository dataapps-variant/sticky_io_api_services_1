-- =============================================================================
-- De-duplicated view: newest row per order_id.
--
-- The pipeline appends rows (safe & simple for a wide, growing schema), so an
-- order can appear more than once if a window is re-run or updated later. Query
-- THIS view for analysis - it keeps only the most recent row per order and
-- exposes every column (device_category, utm_*, billing_*, prd_*, breakdown_*,
-- custom_fields, order_customer_types, raw_order, ...).
--
-- Replace <PROJECT>, <DATASET>, <app> then run.
-- =============================================================================
CREATE OR REPLACE VIEW `<PROJECT>.<DATASET>.orders_<app>_latest` AS
SELECT * EXCEPT(_rn)
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY loaded_at DESC) AS _rn
  FROM `<PROJECT>.<DATASET>.orders_<app>`
)
WHERE _rn = 1;
