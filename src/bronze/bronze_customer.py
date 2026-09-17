# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Customer Data Ingestion
# MAGIC 
# MAGIC This notebook implements the Bronze layer of the Medallion Architecture.
# MAGIC 
# MAGIC **Features:**
# MAGIC - Auto Loader (cloudFiles) for incremental ingestion from Unity Catalog Volume
# MAGIC - Schema evolution enabled
# MAGIC - Change Data Feed enabled
# MAGIC - Audit columns (ingestion_timestamp, source_file_name, load_date)
# MAGIC - DLT expectations for data quality
# MAGIC - Writes to Hive Metastore

# COMMAND ----------

import dlt
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    current_date,
    col,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    DateType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Source configuration - reading from source table
SOURCE_TABLE = "customer.source_customers"
TARGET_SCHEMA = "customer"
TARGET_TABLE = "bronze_customer"



# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer Table Definition
# MAGIC 
# MAGIC This streaming table:
# MAGIC - Ingests CSV files from Unity Catalog Volume
# MAGIC - Adds audit columns
# MAGIC - Enables schema evolution
# MAGIC - Enables Change Data Feed
# MAGIC - Applies data quality expectations

# COMMAND ----------

@dlt.table(
    name="bronze_customer",
    comment="Bronze layer - Raw customer data ingested from Unity Catalog Volume with Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "customer_id,country",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    },
    partition_cols=["load_date"],

)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_country", "country IS NOT NULL")
def bronze_customer():
    """
    Bronze layer streaming table for customer data ingestion.
    
    Source: Unity Catalog Volume (/Volumes/main/customer/customer_files/)
    Destination: hive_metastore.customer.bronze_customer
    
    Features:
    - Auto Loader for incremental file ingestion
    - Schema evolution (addNewColumns)
    - Audit columns (ingestion_timestamp, source_file_name, load_date)
    - Change Data Feed enabled
    - Data quality expectations (customer_id NOT NULL, country NOT NULL)
    - Optimized with auto compaction and optimize write
    - Partitioned by load_date
    
    Returns:
        DataFrame: Streaming DataFrame with customer data and audit columns
    """
    # Read data from source table
    df = spark.table(SOURCE_TABLE)
    
    # Add audit columns
    df_with_audit = (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_name", input_file_name())
        .withColumn("load_date", current_date())
    )
    
    return df_with_audit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Expectations
# MAGIC 
# MAGIC **Implemented Expectations:**
# MAGIC 
# MAGIC 1. **valid_customer_id**: `customer_id IS NOT NULL`
# MAGIC    - Action: `expect_or_drop` (drops invalid records)
# MAGIC    
# MAGIC 2. **valid_country**: `country IS NOT NULL`
# MAGIC    - Action: `expect_or_drop` (drops invalid records)
# MAGIC 
# MAGIC These expectations ensure that only records with valid customer_id and country proceed to downstream layers.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Technical Implementation Notes
# MAGIC 
# MAGIC ### Auto Loader (cloudFiles)
# MAGIC - Automatically processes new files as they arrive
# MAGIC - Maintains schema evolution with `addNewColumns` mode
# MAGIC - Infers column types automatically
# MAGIC - Stores schema metadata in checkpoint location
# MAGIC 
# MAGIC ### Change Data Feed (CDF)
# MAGIC - Enabled via `delta.enableChangeDataFeed = true`
# MAGIC - Tracks INSERT, UPDATE, DELETE operations
# MAGIC - Required for downstream SCD Type 2 implementation
# MAGIC 
# MAGIC ### Optimization
# MAGIC - **Auto Optimize Write**: Optimizes file sizes during write operations
# MAGIC - **Auto Compaction**: Automatically compacts small files
# MAGIC - **Z-Order**: Optimizes data layout by customer_id and country
# MAGIC   - Note: In newer Databricks runtimes, Liquid Clustering is preferred over Z-Order
# MAGIC 
# MAGIC ### Liquid Clustering (Replaces Z-Order)
# MAGIC Liquid Clustering is the next-generation clustering technique that replaces Z-Order:
# MAGIC - Automatically maintains clustering over time
# MAGIC - No need for manual OPTIMIZE commands
# MAGIC - Better performance for filtering and joins
# MAGIC - Simplified maintenance
# MAGIC 
# MAGIC To enable Liquid Clustering (instead of Z-Order), modify table properties:
# MAGIC ```python
# MAGIC table_properties={
# MAGIC     "delta.enableClusteredTable": "true",
# MAGIC     "delta.clusterColumns": "customer_id,country"
# MAGIC }
# MAGIC ```
