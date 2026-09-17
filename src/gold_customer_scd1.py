# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="gold_customer_current",
    comment="Current state of customer dimensions (SCD Type 1)",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_customer_current():
    """
    Gold layer: Current customer dimension (SCD Type 1).
    Source: silver_customer
    Writes to: hive_metastore.customer.gold_customer_current
    Overwrites with latest data on each update.
    """
    return (
        dlt.read_stream("silver_customer")
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("phone"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("status"),
            col("registration_date"),
            current_timestamp().alias("last_updated")
        )
    )
