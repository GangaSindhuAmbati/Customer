# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

@dlt.table(
    name="gold_customer_dim_type1",
    comment="Gold layer - SCD Type 1 customer dimension (current state only)",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableClusteredTable": "true",
        "delta.clusterColumns": "customer_id"
    }
)
def gold_customer_dim_type1():
    return dlt.read("silver_customer").select(
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("country"),
        col("city"),
        current_timestamp().alias("last_updated_at")
    )

dlt.apply_changes(
    target="gold_customer_dim_type1",
    source="silver_customer",
    keys=["customer_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=1
)
