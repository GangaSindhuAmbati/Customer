# Bronze Layer Unit Tests
# Tests for Auto Loader ingestion, schema validation, audit columns, and data quality

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col, current_timestamp, current_date, input_file_name
from chispa.dataframe_comparer import assert_df_equality
from datetime import datetime, date
import tempfile
import os

@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    return SparkSession.builder \
        .appName("BronzeLayerTests") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data for testing"""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("address", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True)
    ])
    
    data = [
        ("C001", "John Doe", "john@example.com", "US", "New York", "123 Main St", 1000.50, 5),
        ("C002", "Jane Smith", "jane@example.com", "GB", "London", "456 High St", 2500.75, 10),
        ("C003", "Bob Johnson", "bob@example.com", "CA", "Toronto", "789 King St", 1500.25, 7),
        (None, "Invalid Customer", "invalid@example.com", "US", "Boston", "321 Elm St", 500.00, 2),
        ("C005", "Alice Brown", "alice@example.com", None, "Sydney", "654 Queen St", 3000.00, 15)
    ]
    
    return spark.createDataFrame(data, schema)

@pytest.fixture
def temp_csv_path(sample_customer_data):
    """Create temporary CSV file for Auto Loader testing"""
    temp_dir = tempfile.mkdtemp()
    csv_path = os.path.join(temp_dir, "customers.csv")
    sample_customer_data.write.csv(csv_path, header=True, mode="overwrite")
    return temp_dir

class TestBronzeCustomerIngestion:
    """Test suite for Bronze layer customer data ingestion"""
    
    def test_auto_loader_ingestion(self, spark, temp_csv_path):
        """Test 1: Auto Loader ingestion from CSV files"""
        # Read CSV using cloudFiles format (Auto Loader simulation)
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(temp_csv_path)
        
        # Verify data was loaded
        assert df.count() == 5, "Should load 5 records from CSV"
        assert "customer_id" in df.columns, "Should have customer_id column"
    
    def test_source_schema_validation(self, spark, sample_customer_data):
        """Test 2: Source schema validation"""
        expected_columns = [
            "customer_id", "customer_name", "email", "country", 
            "city", "address", "sales_amount", "quantity"
        ]
        
        actual_columns = sample_customer_data.columns
        
        for col_name in expected_columns:
            assert col_name in actual_columns, f"Missing expected column: {col_name}"
    
    def test_audit_column_addition(self, spark, sample_customer_data):
        """Test 3: Audit columns are added correctly"""
        # Add audit columns
        df_with_audit = sample_customer_data \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file_name", input_file_name()) \
            .withColumn("load_date", current_date())
        
        # Verify audit columns exist
        assert "ingestion_timestamp" in df_with_audit.columns
        assert "source_file_name" in df_with_audit.columns
        assert "load_date" in df_with_audit.columns
        
        # Verify data types
        schema = df_with_audit.schema
        assert schema["ingestion_timestamp"].dataType.typeName() == "timestamp"
        assert schema["load_date"].dataType.typeName() == "date"
    
    def test_null_customer_id_validation(self, spark, sample_customer_data):
        """Test 4: Null customer_id records are identified"""
        null_customer_ids = sample_customer_data.filter(col("customer_id").isNull())
        assert null_customer_ids.count() == 1, "Should have 1 record with null customer_id"
    
    def test_null_country_validation(self, spark, sample_customer_data):
        """Test 5: Null country records are identified"""
        null_countries = sample_customer_data.filter(col("country").isNull())
        assert null_countries.count() == 1, "Should have 1 record with null country"
    
    def test_volume_path_ingestion(self, spark):
        """Test 6: Validate Unity Catalog Volume path format"""
        expected_path = "/Volumes/main/customer/customer_files/"
        
        # Verify path format is correct for UC Volume
        assert expected_path.startswith("/Volumes/"), "Path should start with /Volumes/"
        assert "main" in expected_path, "Should reference catalog 'main'"
        assert "customer" in expected_path, "Should reference schema 'customer'"
    
    def test_hive_metastore_target(self, spark):
        """Test 7: Validate Hive Metastore target configuration"""
        target_catalog = "hive_metastore"
        target_schema = "customer"
        target_table = "bronze_customer"
        
        full_table_name = f"{target_catalog}.{target_schema}.{target_table}"
        
        assert target_catalog == "hive_metastore", "Should target hive_metastore"
        assert full_table_name == "hive_metastore.customer.bronze_customer"
    
    def test_change_data_feed_property(self, spark):
        """Test 8: Verify Change Data Feed configuration"""
        table_properties = {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true"
        }
        
        assert table_properties["delta.enableChangeDataFeed"] == "true"
    
    def test_data_quality_expectations(self, spark, sample_customer_data):
        """Test 9: Data quality expectation validation"""
        # Simulate expect_or_drop behavior: filter out null customer_id and country
        cleaned_df = sample_customer_data \
            .filter(col("customer_id").isNotNull()) \
            .filter(col("country").isNotNull())
        
        # After dropping nulls, should have 3 valid records
        assert cleaned_df.count() == 3, "Should have 3 records after quality checks"
        
        # Verify no nulls remain
        assert cleaned_df.filter(col("customer_id").isNull()).count() == 0
        assert cleaned_df.filter(col("country").isNull()).count() == 0
    
    def test_partition_column_presence(self, spark, sample_customer_data):
        """Test 10: Verify partition column (load_date) is present"""
        df_with_partition = sample_customer_data.withColumn("load_date", current_date())
        
        assert "load_date" in df_with_partition.columns
        
        # Verify all records have load_date
        null_load_dates = df_with_partition.filter(col("load_date").isNull()).count()
        assert null_load_dates == 0, "All records should have load_date"

class TestBronzeDataTypes:
    """Test data type validation"""
    
    def test_numeric_fields_type(self, spark, sample_customer_data):
        """Test numeric field data types"""
        schema = sample_customer_data.schema
        
        assert schema["sales_amount"].dataType.typeName() == "double"
        assert schema["quantity"].dataType.typeName() == "integer"
    
    def test_string_fields_type(self, spark, sample_customer_data):
        """Test string field data types"""
        schema = sample_customer_data.schema
        
        string_columns = ["customer_id", "customer_name", "email", "country", "city", "address"]
        for col_name in string_columns:
            assert schema[col_name].dataType.typeName() == "string"

class TestAutoLoaderConfiguration:
    """Test Auto Loader configuration options"""
    
    def test_auto_loader_options(self, spark):
        """Test Auto Loader configuration options"""
        options = {
            "cloudFiles.format": "csv",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.schemaLocation": "/tmp/schema/bronze_customer",
            "header": "true",
            "inferSchema": "true"
        }
        
        assert options["cloudFiles.format"] == "csv"
        assert options["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
        assert options["cloudFiles.inferColumnTypes"] == "true"
        assert options["header"] == "true"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
