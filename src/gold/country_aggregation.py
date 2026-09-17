# Databricks notebook source
import dlt
from pyspark.sql.functions import col, count, sum, avg, min, max

@dlt.table(
    name="gold_country_sales",
    comment="Gold layer - Country-level sales aggregation",
    table_properties={
        "quality": "gold",
        "delta.enableClusteredTable": "true",
        "delta.clusterColumns": "country"
    }
)
def gold_country_sales():
    return (
        dlt.read("silver_customer")
        .groupBy("country")
        .agg(
            count("customer_id").alias("total_customers"),
            sum("sales_amount").alias("total_sales"),
            avg("sales_amount").alias("average_sales"),
            min("sales_amount").alias("minimum_sales"),
            max("sales_amount").alias("maximum_sales")
        )
    )
