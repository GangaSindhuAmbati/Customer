# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp, lit

@dlt.table(
    name="bronze_customer",
    comment="Raw customer data ingested from source table with audit columns",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "customer_id"
    }
)
@dlt.expect_all_or_drop({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_email_format": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'"
})
def bronze_customer():
    """
    Bronze layer: Ingest raw customer data from source table.
    Source: workspace.customer.customer_raw_source
    Adds audit columns and applies basic data quality expectations.
    """
    return (
        spark.readStream
        .table("workspace.customer.customer_raw_source")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit("workspace.customer.customer_raw_source"))
    )
