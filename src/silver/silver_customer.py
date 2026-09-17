# Databricks notebook source
import dlt
from pyspark.sql.functions import col, trim, upper, row_number, current_timestamp, when
from pyspark.sql.window import Window

VALID_COUNTRIES = ["US", "GB", "CA", "AU", "DE", "FR", "IT", "ES"]
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'

@dlt.table(
    name="silver_customer",
    table_properties={"delta.enableChangeDataFeed": "true", "delta.enableClusteredTable": "true", "delta.clusterColumns": "customer_id,country"}
)
@dlt.expect_or_fail("valid_email_format", "email_valid = true")
@dlt.expect_or_drop("valid_country_code", "country_valid = true")
@dlt.expect("has_customer_name", "customer_name IS NOT NULL")
@dlt.expect_all({"positive_sales": "sales_amount >= 0"})
def silver_customer():
    df = dlt.read("bronze_customer")
    df_cleaned = df.withColumn("customer_name", trim(col("customer_name"))).withColumn("email", trim(col("email"))).withColumn("country", upper(trim(col("country")))).withColumn("email_valid", col("email").rlike(EMAIL_PATTERN)).withColumn("country_valid", col("country").isin(VALID_COUNTRIES))
    window_spec = Window.partitionBy("customer_id").orderBy(col("ingestion_timestamp").desc())
    return df_cleaned.withColumn("row_num", row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")

@dlt.table(name="silver_customer_rejects")
def silver_customer_rejects():
    df = dlt.read("bronze_customer").withColumn("email", trim(col("email"))).withColumn("country", upper(trim(col("country")))).withColumn("email_valid", col("email").rlike(EMAIL_PATTERN)).withColumn("country_valid", col("country").isin(VALID_COUNTRIES))
    return df.withColumn("rejection_reason", when(~col("email_valid"), "Invalid email").when(~col("country_valid"), "Invalid country").otherwise("Other")).filter((~col("email_valid")) | (~col("country_valid")))
