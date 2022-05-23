-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE monthly_sales_by_state AS
WITH sales AS (
  SELECT
    Store,
    CAST(DATE_TRUNC("month", Date) AS DATE) AS month,
    Sales,
    Customers
  FROM LIVE.sales_silver AS daily_sales
),

store_to_states AS (
  SELECT 
    StateName, Store
  FROM LIVE.store_states_silver
  INNER JOIN LIVE.state_names_silver
  USING (State)
)

SELECT
  StateName,
  Month,
  SUM(Sales) AS MonthlySales,
  SUM(Customers) AS MonthlyCustomers
FROM store_to_states
LEFT JOIN sales
USING (Store)
GROUP BY 1,2
ORDER BY 1,2
