-- Databricks notebook source
-- Materialized View: mv_country_sales
-- Based on gold_country_sales aggregation

CREATE OR REFRESH MATERIALIZED VIEW hive_metastore.customer.mv_country_sales
AS
SELECT
  country,
  total_customers,
  total_sales,
  average_sales,
  minimum_sales,
  maximum_sales
FROM hive_metastore.customer.gold_country_sales;

-- Query the materialized view
SELECT * FROM hive_metastore.customer.mv_country_sales
ORDER BY total_sales DESC;
