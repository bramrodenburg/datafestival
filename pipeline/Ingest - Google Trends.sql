-- Databricks notebook source
CREATE TEMPORARY STREAMING LIVE VIEW googletrends_src AS
SELECT * 
FROM cloud_files("/datafestival/landing/google_trends/", "text")

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE googletrends_bronze
SELECT
  value AS input_row,
  input_file_name() AS input_file_name,
  CURRENT_TIMESTAMP() AS processed_timestamp
FROM STREAM(LIVE.googletrends_src)

-- COMMAND ----------

CREATE OR REPLACE STREAMING LIVE TABLE googletrends_silver AS
WITH parsed_csv AS (
  SELECT
    FROM_CSV(input_row, "file STRING, WEEK STRING, TREND INT") AS parsed_row
  FROM STREAM(LIVE.googletrends_bronze)
),

step2 AS (
  SELECT
    parsed_row.file AS file,
    SPLIT(parsed_row.week, " - ") AS week,
    CAST(parsed_row.trend AS INT) AS trend
  FROM parsed_csv
  WHERE 1=1
    AND parsed_row.file <> 'file' -- Remove headers
)

SELECT
  file,
  CAST(week[0] AS DATE) AS week_start_date,
  CAST(week[1] AS DATE) AS week_end_date,
  trend,
  CURRENT_TIMESTAMP AS processed_timestamp
FROM step2
