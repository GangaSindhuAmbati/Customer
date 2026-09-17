# Databricks notebook source
import dlt
from pyspark.sql.functions import col, trim, upper, when, current_timestamp

@dlt.table(
    name="silver_customer",
    comment="Cleaned and deduplicated customer data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "customer_id"
    }
)
@dlt.expect_all_or_drop({
    "valid_phone": "LENGTH(phone) >= 10",
    "valid_status": "status IN ('active', 'inactive', 'pending')"
})
def silver_customer():
    """
    Silver layer: Clean and deduplicate customer data from bronze layer.
    Source: bronze_customer
    Writes to: hive_metastore.customer.silver_customer
    """
    return (
        dlt.read_stream("bronze_customer")
        .select(
            col("customer_id"),
            trim(col("first_name")).alias("first_name"),
            trim(col("last_name")).alias("last_name"),
            trim(upper(col("email"))).alias("email"),
            col("phone"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("status"),
            col("registration_date"),
            col("ingestion_timestamp")
        )
        .dropDuplicates(["customer_id"])
        .withColumn("processed_timestamp", current_timestamp())
    )

@dlt.table(
    name="silver_customer_quarantine",
    comment="Quarantined records that failed silver layer quality checks"
)
def silver_customer_quarantine():
    """
    Quarantine table for records that fail silver layer expectations.
    Source: bronze_customer
    """
    return (
        dlt.read_stream("bronze_customer")
        .filter(
            (col("phone").isNull()) | (col("status").isNull()) |
            (~col("status").isin(['active', 'inactive', 'pending']))
        )
        .withColumn("quarantine_timestamp", current_timestamp())
    )
