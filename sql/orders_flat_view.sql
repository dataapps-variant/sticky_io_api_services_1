-- ===========================================================================
-- Flat, analyst-friendly view over the raw orders table.
--
-- The ETL stores the WHOLE order in raw_order (JSON text). This view pulls out
-- the columns you use day to day - including the three that the old Airbyte
-- pipeline silently dropped:
--     * utm_info.device_category  (and the other utm_info fields)
--     * custom_fields
--     * order_customer_types
--
-- It also fixes the sale-date bug: date_of_sale is already computed in your
-- reporting timezone by the ETL, so you no longer get the off-by-one-day
-- problem you saw with DATE(time_stamp) on UTC values.
--
-- To add ANY other field sticky.io returns, just add one line here using
-- JSON_VALUE(raw_order, '$.the.path') - no pipeline change needed.
--
-- Replace <PROJECT>, <DATASET> and <app> below, then run.
-- ===========================================================================
CREATE OR REPLACE VIEW `<PROJECT>.<DATASET>.orders_<app>_flat` AS
SELECT
  order_id                                                        AS Order_Id,
  company                                                         AS Company,
  source                                                          AS Source,
  loaded_at                                                       AS Last_Updated,

  -- ---- timing (fixed) ----
  time_stamp                                                      AS Time_Stamp_Raw,
  time_of_sale                                                    AS Time_Of_Sale_UTC,
  date_of_sale                                                    AS Date_Of_Sale,   -- reporting-tz date

  -- ---- customer / billing ----
  JSON_VALUE(raw_order, '$.email_address')                        AS Bill_Email,
  JSON_VALUE(raw_order, '$.billing_first_name')                   AS Bill_First,
  JSON_VALUE(raw_order, '$.billing_last_name')                    AS Bill_Last,
  JSON_VALUE(raw_order, '$.billing_street_address')               AS Bill_Address1,
  JSON_VALUE(raw_order, '$.billing_street_address2')              AS Bill_Address2,
  JSON_VALUE(raw_order, '$.billing_city')                         AS Bill_City,
  JSON_VALUE(raw_order, '$.billing_state')                        AS Bill_State,
  JSON_VALUE(raw_order, '$.billing_postcode')                     AS Bill_Zip,
  JSON_VALUE(raw_order, '$.billing_country')                      AS Bill_Country,
  JSON_VALUE(raw_order, '$.customers_telephone')                  AS Bill_Phone,

  -- ---- shipping ----
  JSON_VALUE(raw_order, '$.shipping_first_name')                  AS Ship_First,
  JSON_VALUE(raw_order, '$.shipping_last_name')                   AS Ship_Last,
  JSON_VALUE(raw_order, '$.shipping_street_address')              AS Ship_Address1,
  JSON_VALUE(raw_order, '$.shipping_city')                        AS Ship_City,
  JSON_VALUE(raw_order, '$.shipping_state')                       AS Ship_State,
  JSON_VALUE(raw_order, '$.shipping_postcode')                    AS Ship_Zip,
  JSON_VALUE(raw_order, '$.shipping_country')                     AS Ship_Country,
  JSON_VALUE(raw_order, '$.shipping_method_name')                 AS Ship_Method_Name,
  SAFE_CAST(JSON_VALUE(raw_order, '$.shipping_amount') AS FLOAT64) AS Ship_Price,

  -- ---- money ----
  SAFE_CAST(JSON_VALUE(raw_order, '$.totals_breakdown.subtotal') AS FLOAT64) AS Sub_Total,
  SAFE_CAST(JSON_VALUE(raw_order, '$.order_total') AS FLOAT64)     AS Order_Total,
  SAFE_CAST(JSON_VALUE(raw_order, '$.order_sales_tax') AS FLOAT64) AS Sales_Tax_Percent,
  SAFE_CAST(JSON_VALUE(raw_order, '$.order_sales_tax_amount') AS FLOAT64) AS Sales_Tax_Amount,

  -- ---- status flags / dates ----
  SAFE_CAST(SAFE_CAST(JSON_VALUE(raw_order, '$.order_status') AS FLOAT64) AS INT64) AS Final_Order_Status,
  JSON_VALUE(raw_order, '$.is_refund')                            AS Is_Refund,
  JSON_VALUE(raw_order, '$.refund_date')                          AS Refund_Date,
  JSON_VALUE(raw_order, '$.is_chargeback')                        AS Is_Chargeback,
  JSON_VALUE(raw_order, '$.chargeback_date')                      AS Chargeback_Date,
  JSON_VALUE(raw_order, '$.is_void')                              AS Is_Void,
  JSON_VALUE(raw_order, '$.void_date')                            AS Void_Date,
  JSON_VALUE(raw_order, '$.is_rma')                               AS Is_RMA,
  JSON_VALUE(raw_order, '$.is_recurring')                         AS Is_Recurring,
  JSON_VALUE(raw_order, '$.recurring_date')                       AS Recurring_Date,

  -- ---- affiliate / tracking ----
  JSON_VALUE(raw_order, '$.afid')                                 AS AFID,
  JSON_VALUE(raw_order, '$.sid')                                  AS SID,
  JSON_VALUE(raw_order, '$.affid')                                AS AFFID,
  JSON_VALUE(raw_order, '$.c1')                                   AS C1,
  JSON_VALUE(raw_order, '$.c2')                                   AS C2,
  JSON_VALUE(raw_order, '$.c3')                                   AS C3,
  JSON_VALUE(raw_order, '$.aid')                                  AS AID,
  JSON_VALUE(raw_order, '$.opt')                                  AS OPT,
  JSON_VALUE(raw_order, '$.campaign_id')                          AS Campaign_Id,

  -- ---- main product (first product in the array) ----
  JSON_VALUE(raw_order, '$.products[0].name')                     AS Product_Name,
  JSON_VALUE(raw_order, '$.products[0].product_id')               AS Product_Id,
  SAFE_CAST(JSON_VALUE(raw_order, '$.products[0].price') AS FLOAT64) AS Product_Price,
  JSON_VALUE(raw_order, '$.products[0].sku')                      AS Product_Sku,
  SAFE_CAST(JSON_VALUE(raw_order, '$.products[0].product_qty') AS INT64) AS Quantity,

  -- ======================================================================
  -- PREVIOUSLY DROPPED FIELDS - now available:
  -- ======================================================================
  JSON_VALUE(raw_order, '$.utm_info.device_category')            AS Device_Category,
  JSON_VALUE(raw_order, '$.utm_info.utm_source')                 AS UTM_Source,
  JSON_VALUE(raw_order, '$.utm_info.utm_medium')                 AS UTM_Medium,
  JSON_VALUE(raw_order, '$.utm_info.utm_campaign')               AS UTM_Campaign,
  JSON_VALUE(raw_order, '$.utm_info.utm_content')               AS UTM_Content,
  JSON_VALUE(raw_order, '$.utm_info.utm_term')                  AS UTM_Term,
  -- keep the whole nested objects too, for anything not broken out above:
  JSON_QUERY(raw_order, '$.utm_info')                            AS UTM_Info_Json,
  JSON_QUERY(raw_order, '$.custom_fields')                       AS Custom_Fields_Json,
  JSON_QUERY(raw_order, '$.order_customer_types')                AS Order_Customer_Types_Json,

  -- the full raw order, in case you need anything else
  raw_order                                                      AS Raw_Order_Json
FROM `<PROJECT>.<DATASET>.orders_<app>`;
