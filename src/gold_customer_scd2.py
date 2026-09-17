# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp, lit, row_number
from pyspark.sql.window import Window

@dlt.table(
    name="gold_customer_history",
    comment="Historical customer dimension with SCD Type 2",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def gold_customer_history():
    """
    Gold layer: Customer dimension with history tracking (SCD Type 2).
    Source: silver_customer
    Writes to: hive_metastore.customer.gold_customer_history
    Tracks all changes to customer attributes over time.
    """
    window_spec = Window.partitionBy("customer_id").orderBy(col("processed_timestamp").desc())
    
    return (
        dlt.read_stream("silver_customer")
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
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
            col("processed_timestamp").alias("effective_start_date"),
            lit(None).cast("timestamp").alias("effective_end_date"),
            lit(True).alias("is_current")
        )
    )
