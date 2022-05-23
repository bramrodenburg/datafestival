-- Databricks notebook source
-- MAGIC %md ### Dataset: `state_names`

-- COMMAND ----------

CREATE TEMPORARY STREAMING LIVE VIEW state_names_landing AS
SELECT * 
FROM cloud_files("/datafestival/landing/state_names/", "text")

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE state_names_bronze
SELECT
  value AS input_row,
  input_file_name() AS input_file_name,
  CURRENT_TIMESTAMP() AS processed_timestamp
FROM STREAM(LIVE.state_names_landing)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE state_names_silver AS
WITH parsed_csv AS (
  SELECT
    FROM_CSV(input_row, "StateName STRING, State STRING") AS parsed_row
  FROM STREAM(LIVE.state_names_bronze)
),

expanded_data AS (
  SELECT
    parsed_row.*
  FROM parsed_csv
)

SELECT
  StateName,
  State
FROM expanded_data
WHERE 1=1

-- COMMAND ----------

-- MAGIC %md ### Dateset: `store_states`

-- COMMAND ----------

CREATE TEMPORARY STREAMING LIVE VIEW store_states_landing AS
SELECT * 
FROM cloud_files("/datafestival/landing/store_states/", "text")

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE store_states_bronze
SELECT
  value AS input_row,
  input_file_name() AS input_file_name,
  CURRENT_TIMESTAMP() AS processed_timestamp
FROM STREAM(LIVE.store_states_landing)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE store_states_silver AS
WITH parsed_csv AS (
  SELECT
    FROM_CSV(input_row, "Store INT, State STRING") AS parsed_row
  FROM STREAM(LIVE.store_states_bronze)
),

expanded_data AS (
  SELECT
    parsed_row.*
  FROM parsed_csv
)

SELECT
  Store,
  State
FROM expanded_data
WHERE 1=1

-- COMMAND ----------

-- MAGIC %md ### Dataset: `stores`

-- COMMAND ----------

CREATE TEMPORARY STREAMING LIVE VIEW stores_landing AS
SELECT * 
FROM cloud_files("/datafestival/landing/stores/", "text")

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE stores_bronze
SELECT
  value AS input_row,
  input_file_name() AS input_file_name,
  CURRENT_TIMESTAMP() AS processed_timestamp
FROM STREAM(LIVE.stores_landing)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE stores_silver AS
WITH parsed_csv2 AS (
  SELECT
    FROM_CSV(input_row, "Store INT, StoreType STRING, Assortment STRING, CompetitionDistance INT, CompetitionOpenSinceMonth INT, CompetitionOpenSinceYear INT, Promo2 STRING, Promo2SinceWeek STRING, Promo2SinceYear STRING, PromoInterval STRING", map("columnNameOfCorruptRecord", "corruptRecord")) AS parsed_row
  FROM STREAM(LIVE.stores_bronze)
),

expanded_data AS (
  SELECT
    parsed_row.*
  FROM parsed_csv2
)

SELECT
  *
FROM expanded_data
WHERE 1=1

-- COMMAND ----------

-- MAGIC %md ### Dataset: `sales`

-- COMMAND ----------

CREATE TEMPORARY STREAMING LIVE VIEW sales_landing AS
SELECT * 
FROM cloud_files("/datafestival/landing/sales/", "text")

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_bronze
SELECT
  value AS input_row,
  input_file_name() AS input_file_name,
  CURRENT_TIMESTAMP() AS processed_timestamp
FROM STREAM(LIVE.sales_landing)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE sales_silver AS
WITH parsed_csv AS (
  SELECT
    FROM_CSV(input_row, "Store INT, DayOfWeek INT, Date DATE, Sales INT, Customers INT, Open INT, Promo INT, StateHoliday INT, SchoolHoliday INT") AS parsed_row
  FROM STREAM(LIVE.sales_bronze)
),

expanded_data AS (
  SELECT
    parsed_row.*
  FROM parsed_csv
)

SELECT
  *
FROM expanded_data
WHERE 1=1

-- COMMAND ----------


