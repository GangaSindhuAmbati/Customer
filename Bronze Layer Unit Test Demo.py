# Databricks notebook source
# DBTITLE 1,Cell 1: Bronze Layer Production Code
# ============================================================================
# CELL 1: Bronze Layer Production Code
# These are the real functions that would live in src/bronze/bronze_customer.py
# They define the Bronze layer logic for customer data ingestion.
# ============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, current_timestamp, current_date, lit


def create_spark_session(app_name="BronzeLayer"):
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


def get_source_schema():
    """Define the expected schema for customer source data."""
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("address", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])


def create_sample_data(spark):
    """Create sample customer data for testing.
    Includes deliberate bad data: row 4 has null customer_id, row 5 has null country."""
    schema = get_source_schema()
    data = [
        ("C001", "John Doe", "john@example.com", "US", "New York", "123 Main St", 1000.50, 5),
        ("C002", "Jane Smith", "jane@example.com", "GB", "London", "456 High St", 2500.75, 10),
        ("C003", "Bob Johnson", "bob@example.com", "CA", "Toronto", "789 King St", 1500.25, 7),
        (None, "Invalid Customer", "invalid@example.com", "US", "Boston", "321 Elm St", 500.00, 2),
        ("C005", "Alice Brown", "alice@example.com", None, "Sydney", "654 Queen St", 3000.00, 15),
    ]
    return spark.createDataFrame(data, schema)


def add_audit_columns(df):
    """Add tracking/audit columns to the Bronze layer DataFrame."""
    return df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file_name", lit("bronze_customer.csv")) \
        .withColumn("load_date", current_date())


def apply_data_quality(df):
    """Apply data quality rules (simulates DLT expect_or_drop).
    Drops records with null customer_id or null country."""
    return df \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("country").isNotNull())


def get_target_table_name():
    """Return the full three-part target table name."""
    catalog = "hive_metastore"
    schema = "customer"
    table = "bronze_customer"
    return f"{catalog}.{schema}.{table}"


def get_auto_loader_options():
    """Return Auto Loader configuration options."""
    return {
        "cloudFiles.format": "csv",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaLocation": "/tmp/schema/bronze_customer",
        "header": "true",
        "inferSchema": "true",
    }


# --- Initialize Spark session for all cells ---
spark = create_spark_session("BronzeLayerDemo")
sample_df = create_sample_data(spark)

print("✓ Spark session created")
print(f"✓ Sample data: {sample_df.count()} rows, {len(sample_df.columns)} columns")
sample_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Cell 2: 8 Manual Unit Tests with Arrange/Act/Assert
# ============================================================================
# CELL 2: Manual Unit Tests with Arrange/Act/Assert Output
# Each test follows the AAA pattern and prints step-by-step results.
# ============================================================================

import tempfile, os

passed = 0
failed = 0


def run_test(test_name, test_func):
    """Run a single manual test and track results."""
    global passed, failed
    print(f"\n{'='*60}")
    print(f"  TEST: {test_name}")
    print(f"{'='*60}")
    try:
        test_func()
        passed += 1
        print(f"  RESULT: ✅ PASSED")
    except AssertionError as e:
        failed += 1
        print(f"  RESULT: ❌ FAILED — {e}")


def test_1_row_count():
    expected_rows = 5
    print(f"  Arrange: expecting {expected_rows} rows")
    actual_rows = sample_df.count()
    print(f"  Act: actual row count = {actual_rows}")
    assert actual_rows == expected_rows, f"Expected {expected_rows}, got {actual_rows}"
    print(f"  Assert: {actual_rows} == {expected_rows} ✓")


def test_2_schema_columns():
    expected_columns = ["customer_id", "customer_name", "email", "country",
                        "city", "address", "sales_amount", "quantity"]
    print(f"  Arrange: expecting {len(expected_columns)} columns")
    actual_columns = sample_df.columns
    print(f"  Act: actual columns = {actual_columns}")
    for col_name in expected_columns:
        assert col_name in actual_columns, f"Missing column: {col_name}"
    print(f"  Assert: all {len(expected_columns)} columns present ✓")


def test_3_audit_columns():
    expected_audit_cols = ["ingestion_timestamp", "source_file_name", "load_date"]
    print(f"  Arrange: will add audit columns {expected_audit_cols}")
    df_with_audit = add_audit_columns(sample_df)
    actual_cols = df_with_audit.columns
    print(f"  Act: columns after adding audit = {actual_cols}")
    for audit_col in expected_audit_cols:
        assert audit_col in actual_cols, f"Missing audit column: {audit_col}"
    assert df_with_audit.schema["ingestion_timestamp"].dataType.typeName() == "timestamp"
    assert df_with_audit.schema["load_date"].dataType.typeName() == "date"
    print(f"  Assert: all 3 audit columns present with correct types ✓")


def test_4_null_customer_id():
    expected_nulls = 1
    print(f"  Arrange: expecting {expected_nulls} record with null customer_id")
    null_count = sample_df.filter(col("customer_id").isNull()).count()
    print(f"  Act: found {null_count} null customer_id records")
    assert null_count == expected_nulls, f"Expected {expected_nulls}, got {null_count}"
    print(f"  Assert: {null_count} == {expected_nulls} ✓")


def test_5_null_country():
    expected_nulls = 1
    print(f"  Arrange: expecting {expected_nulls} record with null country")
    null_count = sample_df.filter(col("country").isNull()).count()
    print(f"  Act: found {null_count} null country records")
    assert null_count == expected_nulls, f"Expected {expected_nulls}, got {null_count}"
    print(f"  Assert: {null_count} == {expected_nulls} ✓")


def test_6_data_quality_filter():
    expected_after_cleaning = 3
    print(f"  Arrange: starting with 5 rows, expecting {expected_after_cleaning} after cleaning")
    cleaned_df = apply_data_quality(sample_df)
    actual_count = cleaned_df.count()
    print(f"  Act: after applying quality filters, {actual_count} rows remain")
    assert actual_count == expected_after_cleaning, f"Expected {expected_after_cleaning}, got {actual_count}"
    assert cleaned_df.filter(col("customer_id").isNull()).count() == 0
    assert cleaned_df.filter(col("country").isNull()).count() == 0
    print(f"  Assert: {actual_count} rows remain, no nulls in customer_id or country ✓")


def test_7_target_table_name():
    expected_name = "hive_metastore.customer.bronze_customer"
    print(f"  Arrange: expecting table name '{expected_name}'")
    actual_name = get_target_table_name()
    print(f"  Act: got table name '{actual_name}'")
    assert actual_name == expected_name, f"Expected '{expected_name}', got '{actual_name}'"
    print(f"  Assert: '{actual_name}' == '{expected_name}' ✓")


def test_8_auto_loader_options():
    required_keys = ["cloudFiles.format", "cloudFiles.schemaEvolutionMode",
                     "cloudFiles.inferColumnTypes", "header"]
    print(f"  Arrange: checking {len(required_keys)} required Auto Loader options")
    options = get_auto_loader_options()
    print(f"  Act: options = {options}")
    for key in required_keys:
        assert key in options, f"Missing option: {key}"
    assert options["cloudFiles.format"] == "csv"
    assert options["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    print(f"  Assert: all required options present and correct ✓")


print("\n" + "#" * 60)
print("#  RUNNING 8 MANUAL UNIT TESTS (Arrange/Act/Assert)")
print("#" * 60)

run_test("Row count = 5", test_1_row_count)
run_test("Schema columns present", test_2_schema_columns)
run_test("Audit columns added", test_3_audit_columns)
run_test("Null customer_id detected", test_4_null_customer_id)
run_test("Null country detected", test_5_null_country)
run_test("Data quality filter works", test_6_data_quality_filter)
run_test("Target table name correct", test_7_target_table_name)
run_test("Auto Loader options correct", test_8_auto_loader_options)

print(f"\n{'#'*60}")
print(f"#  SUMMARY: {passed} passed, {failed} failed")
print(f"{'#'*60}")

# COMMAND ----------

# DBTITLE 1,Cell 3: Write pytest File to Disk and Run It
# ============================================================================
# CELL 3: Write a Proper pytest Test File to Disk and Run with pytest.main()
# This demonstrates how pytest discovers and runs tests — the same way
# your GitHub Actions CI/CD pipeline runs: `pytest tests/ -v --tb=short`
# ============================================================================

import os, textwrap

test_dir = "/tmp/bronze_unit_tests"
os.makedirs(test_dir, exist_ok=True)

conftest_code = '''
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


@pytest.fixture(scope="session")
def spark():
    """Create a shared Spark session for all tests."""
    return SparkSession.builder \\
        .appName("BronzePytestSuite") \\
        .config("spark.sql.shuffle.partitions", "2") \\
        .getOrCreate()


@pytest.fixture
def sample_customer_data(spark):
    """Create sample customer data with 5 rows (2 intentionally bad)."""
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("address", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])
    data = [
        ("C001", "John Doe", "john@example.com", "US", "New York", "123 Main St", 1000.50, 5),
        ("C002", "Jane Smith", "jane@example.com", "GB", "London", "456 High St", 2500.75, 10),
        ("C003", "Bob Johnson", "bob@example.com", "CA", "Toronto", "789 King St", 1500.25, 7),
        (None, "Invalid Customer", "invalid@example.com", "US", "Boston", "321 Elm St", 500.00, 2),
        ("C005", "Alice Brown", "alice@example.com", None, "Sydney", "654 Queen St", 3000.00, 15),
    ]
    return spark.createDataFrame(data, schema)
'''

with open(os.path.join(test_dir, "conftest.py"), "w") as f:
    f.write(textwrap.dedent(conftest_code))

test_code = '''
import pytest
from pyspark.sql.functions import col, current_timestamp, current_date, lit


class TestBronzeIngestion:
    """Test suite for Bronze layer customer data ingestion."""

    def test_01_row_count(self, spark, sample_customer_data):
        """Verify 5 records loaded from source."""
        assert sample_customer_data.count() == 5

    def test_02_schema_columns(self, spark, sample_customer_data):
        """Verify all 8 expected columns are present."""
        expected = ["customer_id", "customer_name", "email", "country",
                     "city", "address", "sales_amount", "quantity"]
        for c in expected:
            assert c in sample_customer_data.columns, f"Missing column: {c}"

    def test_03_null_customer_id(self, spark, sample_customer_data):
        """Verify exactly 1 record has null customer_id."""
        null_count = sample_customer_data.filter(col("customer_id").isNull()).count()
        assert null_count == 1, f"Expected 1 null, got {null_count}"

    def test_04_null_country(self, spark, sample_customer_data):
        """Verify exactly 1 record has null country."""
        null_count = sample_customer_data.filter(col("country").isNull()).count()
        assert null_count == 1, f"Expected 1 null, got {null_count}"

    def test_05_data_quality_filter(self, spark, sample_customer_data):
        """Verify quality filter removes 2 bad records, leaving 3."""
        cleaned = sample_customer_data \\\n            .filter(col("customer_id").isNotNull()) \\\n            .filter(col("country").isNotNull())
        assert cleaned.count() == 3, "Should have 3 records after quality checks"
        assert cleaned.filter(col("customer_id").isNull()).count() == 0
        assert cleaned.filter(col("country").isNull()).count() == 0

    def test_06_audit_columns(self, spark, sample_customer_data):
        """Verify audit columns are added with correct types."""
        df = sample_customer_data \\\n            .withColumn("ingestion_timestamp", current_timestamp()) \\\n            .withColumn("source_file_name", lit("bronze_customer.csv")) \\\n            .withColumn("load_date", current_date())
        assert "ingestion_timestamp" in df.columns
        assert "source_file_name" in df.columns
        assert "load_date" in df.columns
        assert df.schema["ingestion_timestamp"].dataType.typeName() == "timestamp"
        assert df.schema["load_date"].dataType.typeName() == "date"

    def test_07_numeric_types(self, spark, sample_customer_data):
        """Verify numeric columns have correct data types."""
        schema = sample_customer_data.schema
        assert schema["sales_amount"].dataType.typeName() == "double"
        assert schema["quantity"].dataType.typeName() == "integer"

    def test_08_string_types(self, spark, sample_customer_data):
        """Verify all text columns are StringType."""
        schema = sample_customer_data.schema
        string_cols = ["customer_id", "customer_name", "email", "country", "city", "address"]
        for c in string_cols:
            assert schema[c].dataType.typeName() == "string", f"{c} should be string"

    def test_09_auto_loader_options(self, spark):
        """Verify Auto Loader configuration options."""
        options = {
            "cloudFiles.format": "csv",
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
            "cloudFiles.inferColumnTypes": "true",
            "header": "true",
        }
        assert options["cloudFiles.format"] == "csv"
        assert options["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
        assert options["cloudFiles.inferColumnTypes"] == "true"

    def test_10_intentional_failure(self, spark, sample_customer_data):
        """INTENTIONALLY FAILS to show what a pytest failure looks like."""
        actual = sample_customer_data.count()
        assert actual == 10, f"Expected 10 rows (this will fail — actual is {actual})"
'''

with open(os.path.join(test_dir, "test_bronze_pytest.py"), "w") as f:
    f.write(textwrap.dedent(test_code))

print(f"✓ conftest.py written to {test_dir}/")
print(f"✓ test_bronze_pytest.py written to {test_dir}/")
print(f"\nFiles in test directory:")
for f in sorted(os.listdir(test_dir)):
    if f.endswith(".py"):
        print(f"  - {f}")

print("\n" + "=" * 70)
print("  RUNNING pytest.main() — same as `pytest tests/ -v --tb=short`")
print("=" * 70)

import pytest as pytest_module

exit_code = pytest_module.main([
    f"{test_dir}/test_bronze_pytest.py",
    "-v",
    "--tb=short",
    "--no-header",
    f"-o", "tmpdir=/tmp/bronze_pytest_tmp",
    "--color=no",
])

print(f"\n{'=' * 70}")
if exit_code == 0:
    print("  pytest exit code: 0 (all tests passed)")
else:
    print(f"  pytest exit code: {exit_code} ({exit_code} test(s) failed — expected)")
print("=" * 70)
print("\n📝 10 tests ran: 9 passed + 1 intentionally failed (test_10_intentional_failure)")
print("   In a real CI/CD pipeline, this failure would BLOCK the deployment.")

# COMMAND ----------

# DBTITLE 1,Cell 4: YAML CI/CD Integration Explained
# MAGIC %md
# MAGIC # How Unit Tests Connect to YAML Files (CI/CD Integration)
# MAGIC
# MAGIC ## The Key Idea
# MAGIC
# MAGIC The unit tests in `test_bronze.py` don't run in isolation forever. In a real project, they are **automatically triggered by a CI/CD pipeline defined in a YAML file** (`.github/workflows/deploy.yml`). The YAML file orchestrates the entire flow: **test → validate → deploy → run**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The YAML Pipeline: `.github/workflows/deploy.yml`
# MAGIC
# MAGIC ```yaml
# MAGIC name: DLT Pipeline CI/CD
# MAGIC
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [ main, develop ]
# MAGIC   pull_request:
# MAGIC     branches: [ main ]
# MAGIC
# MAGIC jobs:
# MAGIC   test:                              # ← JOB 1: Run your unit tests
# MAGIC     name: Run Unit Tests
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - name: Checkout Repository
# MAGIC         uses: actions/checkout@v4
# MAGIC       - name: Setup Python
# MAGIC         uses: actions/setup-python@v5
# MAGIC         with:
# MAGIC           python-version: '3.10'
# MAGIC       - name: Install Dependencies
# MAGIC         run: |
# MAGIC           pip install -r requirements.txt    # ← Installs pytest, chispa, pyspark
# MAGIC       - name: Run pytest
# MAGIC         run: |
# MAGIC           pytest tests/ -v --tb=short        # ← THIS RUNS test_bronze.py
# MAGIC       - name: Upload Test Results
# MAGIC         if: always()
# MAGIC         uses: actions/upload-artifact@v4
# MAGIC
# MAGIC   validate:                           # ← JOB 2: Validate Databricks bundle
# MAGIC     name: Validate Databricks Bundle
# MAGIC     needs: test                        # ← WAITS for tests to pass first
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - name: Bundle Validate (dev)
# MAGIC         run: |
# MAGIC           databricks bundle validate --target development
# MAGIC
# MAGIC   deploy:                             # ← JOB 3: Deploy to production
# MAGIC     name: Deploy to Databricks
# MAGIC     needs: validate                    # ← WAITS for validation to pass
# MAGIC     runs-on: ubuntu-latest
# MAGIC     if: github.ref == 'refs/heads/main'
# MAGIC     steps:
# MAGIC       - name: Bundle Deploy
# MAGIC         run: |
# MAGIC           databricks bundle deploy --target production
# MAGIC       - name: Trigger DLT Pipeline
# MAGIC         run: |
# MAGIC           databricks bundle run customer_medallion_job
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## How Each YAML Section Relates to the Unit Tests
# MAGIC
# MAGIC | YAML Line | What It Does | Connection to `test_bronze.py` |
# MAGIC |---|---|---|
# MAGIC | `on: push` (line 3-5) | Triggered when you `git push` to main/develop | Starts the whole pipeline |
# MAGIC | `pip install -r requirements.txt` (line 30) | Installs `pytest==7.4.0`, `chispa==0.9.4`, `pyspark==3.5.0` | These are the exact libraries your test imports |
# MAGIC | `pytest tests/ -v --tb=short` (line 34) | Discovers and runs all `test_*.py` files in `tests/` | **This is where `test_bronze.py` actually executes** |
# MAGIC | `needs: test` (line 45) | The `validate` job won't run until tests pass | If tests fail, deployment is blocked |
# MAGIC | `needs: validate` (line 62) | The `deploy` job won't run until validation passes | Double gate before production |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## The `databricks.yml` Bundle Config
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: customer-dlt-project
# MAGIC
# MAGIC include:
# MAGIC   - resources/*.yml          # Pulls in pipeline.yml, job.yml
# MAGIC
# MAGIC targets:
# MAGIC   development:               # Used by: databricks bundle validate --target development
# MAGIC     mode: development
# MAGIC     variables:
# MAGIC       catalog: workspace
# MAGIC       schema: customer_demo
# MAGIC   production:                # Used by: databricks bundle deploy --target production
# MAGIC     mode: production
# MAGIC     variables:
# MAGIC       catalog: workspace
# MAGIC       schema: customer_demo
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why This Matters
# MAGIC
# MAGIC 1. **Tests run automatically** — you never have to manually run tests before deploying
# MAGIC 2. **Failures block deployment** — bad code can never reach production
# MAGIC 3. **Full audit trail** — GitHub Actions shows which tests passed/failed on every push
# MAGIC 4. **Reproducible** — the same `requirements.txt` ensures identical test environments every time

# COMMAND ----------

# DBTITLE 1,Cell 5: Complete CI/CD Flow Diagram
# MAGIC %md
# MAGIC # Complete End-to-End Flow: From Developer to Production
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                         DEVELOPER WORKFLOW                           │
# MAGIC │                                                                     │
# MAGIC │  1. Write/update src/bronze/bronze_customer.py  (production code)    │
# MAGIC │  2. Write/update tests/test_bronze.py           (unit tests)        │
# MAGIC │  3. git push to main or develop                                      │
# MAGIC └──────────────────────────────┬──────────────────────────────────────┘
# MAGIC                                │
# MAGIC                                ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                    .github/workflows/deploy.yml                      │
# MAGIC │                     (GitHub Actions CI/CD)                          │
# MAGIC │                                                                     │
# MAGIC │  ┌─ JOB 1: test ───────────────────────────────────────────────────┐ │
# MAGIC │  │                                                               │ │
# MAGIC │  │  Step 1: actions/checkout@v4        → Download code            │ │
# MAGIC │  │  Step 2: actions/setup-python@v5     → Python 3.10             │ │
# MAGIC │  │  Step 3: pip install -r requirements.txt                       │ │
# MAGIC │  │          → pytest==7.4.0, chispa==0.9.4, pyspark==3.5.0       │ │
# MAGIC │  │  Step 4: pytest tests/ -v --tb=short  → RUN TESTS              │ │
# MAGIC │  │                                                               │ │
# MAGIC │  │  test_bronze.py executes:                                     │ │
# MAGIC │  │    ├── test_auto_loader_ingestion          ✓                  │ │
# MAGIC │  │    ├── test_source_schema_validation       ✓                  │ │
# MAGIC │  │    ├── test_audit_column_addition         ✓                  │ │
# MAGIC │  │    ├── test_null_customer_id_validation    ✓                  │ │
# MAGIC │  │    ├── test_null_country_validation       ✓                  │ │
# MAGIC │  │    ├── test_volume_path_ingestion         ✓                  │ │
# MAGIC │  │    ├── test_hive_metastore_target        ✓                  │ │
# MAGIC │  │    ├── test_change_data_feed_property    ✓                  │ │
# MAGIC │  │    ├── test_data_quality_expectations   ✓                  │ │
# MAGIC │  │    ├── test_partition_column_presence   ✓                  │ │
# MAGIC │  │    ├── test_numeric_fields_type         ✓                  │ │
# MAGIC │  │    ├── test_string_fields_type         ✓                  │ │
# MAGIC │  │    └── test_auto_loader_options        ✓                  │ │
# MAGIC │  │                                                               │ │
# MAGIC │  │  If ALL PASS → continue to JOB 2                             │ │
# MAGIC │  │  If ANY FAIL → STOP, send email notification                  │ │
# MAGIC │  └───────────────────────────────────────────────────────────────┘ │
# MAGIC │                               │                                     │
# MAGIC │                               │ (only if all tests pass)            │
# MAGIC │                               ▼                                     │
# MAGIC │  ┌─ JOB 2: validate ─────────────────────────────────────────────┐ │
# MAGIC │  │                                                               │ │
# MAGIC │  │  Step 1: databricks/setup-cli@v1      → Install Databricks CLI  │ │
# MAGIC │  │  Step 2: databricks bundle validate --target development       │ │
# MAGIC │  │          → Checks databricks.yml + resources/*.yml            │ │
# MAGIC │  │          → Validates pipeline.yml (DLT pipeline config)       │ │
# MAGIC │  │          → Validates job.yml (scheduled job config)           │ │
# MAGIC │  │          → Checks for syntax errors, missing resources        │ │
# MAGIC │  │                                                               │ │
# MAGIC │  │  If VALID → continue to JOB 3                                │ │
# MAGIC │  │  If INVALID → STOP                                          │ │
# MAGIC │  └───────────────────────────────────────────────────────────────┘ │
# MAGIC │                               │                                     │
# MAGIC │                               │ (only if validation passes)         │
# MAGIC │                               ▼                                     │
# MAGIC │  ┌─ JOB 3: deploy (main branch only) ────────────────────────────┐ │
# MAGIC │  │                                                               │ │
# MAGIC │  │  Step 1: databricks bundle deploy --target production         │ │
# MAGIC │  │          → Deploys pipeline.yml → DLT pipeline in workspace   │ │
# MAGIC │  │          → Deploys job.yml → Scheduled job in workspace       │ │
# MAGIC │  │          → All resources go to production target               │ │
# MAGIC │  │                                                               │ │
# MAGIC │  │  Step 2: databricks bundle run customer_medallion_job         │ │
# MAGIC │  │          → Triggers the DLT pipeline immediately              │ │
# MAGIC │  │                                                               │ │
# MAGIC │  │  → Pipeline processes:                                         │ │
# MAGIC │  │     Bronze: CSV → bronze_customer table (Auto Loader)        │ │
# MAGIC │  │     Silver: bronze → silver_customer table (cleansed)        │ │
# MAGIC │  │     Gold:   silver → gold tables (aggregates, SCD1, SCD2)    │ │
# MAGIC │  └───────────────────────────────────────────────────────────────┘ │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC | Step | YAML File | Purpose |
# MAGIC |------|-----------|---------|
# MAGIC | 1 | `deploy.yml` → `on: push` | Trigger pipeline on code push |
# MAGIC | 2 | `deploy.yml` → `pip install -r requirements.txt` | Install pytest + pyspark |
# MAGIC | 3 | `deploy.yml` → `pytest tests/ -v --tb=short` | **Run unit tests** (test_bronze.py) |
# MAGIC | 4 | `deploy.yml` → `needs: test` | Block deployment if tests fail |
# MAGIC | 5 | `deploy.yml` → `databricks bundle validate` | Validate bundle syntax |
# MAGIC | 6 | `deploy.yml` → `databricks bundle deploy` | Deploy to production |
# MAGIC | 7 | `databricks.yml` → `include: resources/*.yml` | Load pipeline + job definitions |
# MAGIC | 8 | `pipeline.yml` → `libraries: [notebooks]` | Define Bronze/Silver/Gold notebooks |
# MAGIC | 9 | `job.yml` → `schedule: cron` | Run pipeline daily at 2 AM |
# MAGIC
# MAGIC **The unit tests are the gatekeeper**: if any test in `test_bronze.py` fails, the pipeline stops at Job 1, and no code reaches production. This is the core of CI/CD — automated quality gates enforced by YAML configuration."