-- =============================================================================
-- Drop-in replacement for the old `Sticky_data_API_original_CT` view.
--
-- Same output column names your downstream queries already use, but sourced from
-- the NEW Airbyte-free wide table. Two things are fixed/added versus the old view:
--   * Date_of_Sale now uses date_of_sale (correct reporting-timezone date),
--     instead of DATE(time_stamp) which was the UTC value and drifted by a day.
--   * Device_Category (and UTM fields) are now available - the old pipeline
--     dropped utm_info entirely.
--
-- De-duplication (newest row per order) is handled by orders_<app>_latest, so
-- you don't need the ROW_NUMBER block the old view had.
--
-- Replace <PROJECT>, <DATASET>, <app> then run.
-- =============================================================================
CREATE OR REPLACE VIEW `<PROJECT>.<DATASET>.Sticky_data_API_original_CT` AS
SELECT
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', loaded_at)              AS last_updated,
  order_id                                                      AS Order_Id,
  billing_first_name                                            AS Bill_First,
  billing_last_name                                             AS Bill_Last,
  billing_street_address                                        AS Bill_Address1,
  billing_street_address2                                       AS Bill_Address2,
  billing_city                                                  AS Bill_City,
  billing_state                                                 AS Bill_State,
  billing_postcode                                              AS Bill_Zip,
  billing_country                                               AS Bill_Country,
  customers_telephone                                           AS Bill_Phone,
  email_address                                                 AS Bill_Email,
  shipping_first_name                                           AS Ship_First,
  shipping_last_name                                            AS Ship_Last,
  shipping_street_address                                       AS Ship_Address1,
  shipping_street_address2                                      AS Ship_Address2,
  shipping_city                                                 AS Ship_City,
  shipping_state                                                AS Ship_State,
  shipping_postcode                                             AS Ship_Zip,
  shipping_country                                              AS Ship_Country,
  shipping_method_name                                          AS Ship_Method_Name,
  shipping_amount                                               AS Ship_Price,
  breakdown_subtotal                                            AS Sub_Total,
  order_sales_tax                                               AS Sales_Tax_Percent,
  order_sales_tax_amount                                        AS Sales_Tax_Factor,
  order_total                                                   AS Order_Total,

  -- FIXED: correct reporting-timezone sale date (was DATE(time_stamp) on UTC)
  date_of_sale                                                  AS Date_of_Sale,
  time_stamp                                                    AS Time_Stamp_Raw,
  time_of_sale                                                  AS Time_Of_Sale,

  tracking_number                                               AS Tracking_Number,
  cc_type                                                       AS Payment,
  campaign_id                                                   AS Campaign_Id,
  customer_id                                                   AS Customer_Number,
  credit_card_number                                            AS Credit_Card_Number,
  cc_expires                                                    AS Credit_Card_Expiration,
  prepaid_match                                                 AS Prepaid_Match,
  gateway_id                                                    AS Gateway_Id,
  gateway_descriptor                                            AS Gateway_Descriptor,
  processor_id                                                  AS Processor_Id,
  ip_address                                                    AS IP_Address,
  ip_Address_lookup                                             AS IP_Address_Lookup,
  SAFE_CAST(SAFE_CAST(order_status AS FLOAT64) AS INT64)        AS Final_Order_Status,
  decline_reason                                                AS Decline_Reason,
  is_cascaded                                                   AS Is_Cascaded,
  is_fraud                                                      AS Is_Fraud,
  is_chargeback                                                 AS Is_Chargeback,
  chargeback_date                                               AS Chargeback_Date,
  is_rma                                                        AS Is_RMA,
  rma_number                                                    AS RMA_Number,
  rma_reason                                                    AS RMA_Reason,
  return_reason                                                 AS Return_Reason,
  is_recurring                                                  AS Is_Recurring,
  recurring_date                                                AS Recurring_Date,
  retry_date                                                    AS Retry_Date,
  transaction_id                                                AS Transaction_Number,
  auth_id                                                       AS Auth_Number,
  retry_attempt                                                 AS Retry_Attempt,
  hold_date                                                     AS Hold_Date,
  is_void                                                       AS Is_Void,
  void_amount                                                   AS Void_Amount,
  void_date                                                     AS Void_Date,
  is_refund                                                     AS Is_Refund,
  refund_amount                                                 AS Refund_Amount,
  refund_date                                                   AS Refund_Date,
  afid                                                          AS AFID,
  sid                                                           AS SID,
  affid                                                         AS AFFID,
  c1                                                            AS C1,
  c2                                                            AS C2,
  c3                                                            AS C3,
  aid                                                           AS AID,
  opt                                                           AS OPT,
  rebill_discount_percent                                       AS Rebill_Discount,
  billing_cycle                                                 AS Billing_Cycle,
  parent_id                                                     AS Parent_Order_Id,
  main_product_id                                               AS Product_Id,
  prd_name                                                      AS Product_Name,
  prd_price                                                     AS Product_Price,
  prd_sku                                                       AS Product_Sku,
  prd_product_qty                                               AS Quantity,
  acquisition_date                                              AS Acquisition_Date_Time,
  is_blacklisted                                                AS Blacklisted,
  ancestor_id                                                   AS Ancestor_Order_Id,
  decline_salvage_discount_percent                              AS Decline_Salvage_Discount_per,
  is_test_cc                                                    AS Test,
  prd_hold_type                                                 AS Hold_Type,
  prd_offer_id                                                  AS Offer_Id,
  cc_first_6                                                    AS BIN,

  -- ADDED: fields the old Airbyte pipeline dropped
  device_category                                               AS Device_Category,
  utm_source                                                    AS UTM_Source,
  utm_medium                                                    AS UTM_Medium,
  utm_campaign                                                  AS UTM_Campaign,
  custom_fields                                                 AS Custom_Fields_Json,
  order_customer_types                                          AS Order_Customer_Types_Json
FROM `<PROJECT>.<DATASET>.orders_<app>_latest`;
