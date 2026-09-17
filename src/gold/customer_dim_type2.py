# Databricks notebook source
import dlt

@dlt.table(
    name="gold_customer_dim_type2",
    comment="Gold layer - SCD Type 2 customer dimension (full history)",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableClusteredTable": "true",
        "delta.clusterColumns": "customer_id"
    }
)
def gold_customer_dim_type2():
    return dlt.read("silver_customer")

dlt.apply_changes(
    target="gold_customer_dim_type2",
    source="silver_customer",
    keys=["customer_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type=2,
    track_history_column_list=["customer_name", "email", "country", "city"]
)
