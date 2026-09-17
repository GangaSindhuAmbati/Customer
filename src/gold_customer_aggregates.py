# Databricks notebook source
import dlt
from pyspark.sql.functions import col, count, countDistinct

@dlt.table(
    name="gold_customer_by_state",
    comment="Customer aggregations by state - Materialized View",
    table_properties={
        "quality": "gold"
    }
)
def gold_customer_by_state():
    """
    Gold layer: Aggregated customer metrics by state.
    Source: silver_customer
    Writes to: hive_metastore.customer.gold_customer_by_state
    Materialized view for reporting.
    """
    return (
        dlt.read("silver_customer")
        .groupBy("state", "status")
        .agg(
            count("*").alias("customer_count"),
            countDistinct("customer_id").alias("unique_customers")
        )
        .orderBy("state", "status")
    )
