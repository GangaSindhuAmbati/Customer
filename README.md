# Customer DLT Project - Medallion Architecture

A complete end-to-end Delta Live Tables (DLT) project implementing the Medallion Architecture (Bronze, Silver, Gold) for customer data processing.

## Project Overview

This project demonstrates a production-ready data pipeline using:
- **Delta Live Tables (DLT)** for declarative data transformations
- **Medallion Architecture** (Bronze, Silver, Gold)
- **Auto Loader** for incremental file ingestion
- **Unity Catalog Volumes** for source data storage
- **Hive Metastore** for destination tables
- **GitHub Actions** for CI/CD
- **Databricks Asset Bundles** for deployment

## Architecture

```
Unity Catalog Volume -> Bronze -> Silver -> Gold -> Materialized Views
     (CSV files)        (Raw)   (Clean)  (Curated)
```

### Data Flow

1. **Bronze Layer**: Raw data ingestion from `/Volumes/main/customer/customer_files/`
2. **Silver Layer**: Data quality, deduplication, validation
3. **Gold Layer**: Business-level aggregations and SCD dimensions

## Project Structure

```
customer-dlt-project/
|
|-- src/
|   |-- bronze/
|   |   |-- bronze_customer.py          # Auto Loader ingestion
|   |-- silver/
|   |   |-- silver_customer.py          # Data quality & deduplication
|   |-- gold/
|       |-- customer_dim_type1.py       # SCD Type 1 dimension
|       |-- customer_dim_type2.py       # SCD Type 2 dimension
|       |-- country_aggregation.py      # Country-level metrics
|       |-- materialized_views.sql       # Materialized views
|
|-- tests/
|   |-- test_bronze.py                  # Pytest unit tests
|
|-- resources/
|   |-- pipeline.yml                    # DLT Pipeline config
|   |-- job.yml                         # Job config
|
|-- .github/
|   |-- workflows/
|       |-- deploy.yml                  # CI/CD pipeline
|
|-- databricks.yml                      # Databricks Asset Bundle
|-- requirements.txt                    # Python dependencies
|-- README.md                           # This file
```

## Deployment Instructions

### Prerequisites

1. **Databricks Workspace** (Free Edition supported)
2. **Unity Catalog** enabled with:
   - Catalog: `main`
   - Schema: `customer`
   - Volume: `customer_files`
3. **GitHub Repository** with secrets:
   - `DATABRICKS_TOKEN`
4. **Databricks CLI** installed

### Setup Steps

#### 1. Clone Repository

```bash
git clone https://github.com/GangaSindhuAmbati/Customer.git
cd Customer
```

#### 2. Configure Databricks Bundle

Edit `databricks.yml`:

```yaml
targets:
  default:
    workspace:
      host: https://your-workspace.cloud.databricks.com
```

#### 3. Upload Source Data

Place CSV files in Unity Catalog Volume:

```
/Volumes/main/customer/customer_files/customers_2024_01.csv
/Volumes/main/customer/customer_files/customers_2024_02.csv
```

**Expected CSV Schema:**

```csv
customer_id,customer_name,email,country,city,address,sales_amount,quantity
C001,John Doe,john@example.com,US,New York,123 Main St,1000.50,5
C002,Jane Smith,jane@example.com,GB,London,456 High St,2500.75,10
```

#### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

#### 5. Run Unit Tests

```bash
pytest tests/ -v
```

#### 6. Validate Bundle

```bash
databricks bundle validate
```

#### 7. Deploy Bundle

```bash
databricks bundle deploy --target default
```

#### 8. Run DLT Pipeline

```bash
databricks bundle run customer_medallion_pipeline
```

## Testing

### Run Unit Tests

```bash
pytest tests/test_bronze.py -v
```

### Test Coverage

- Auto Loader ingestion
- Schema validation
- Audit column addition
- Null value handling
- Data quality expectations
- Change Data Feed configuration
- Partition column validation

## CI/CD Pipeline

### GitHub Actions Workflow

1. **Test** -> Run pytest on push/PR
2. **Validate** -> Validate Databricks Bundle
3. **Deploy** -> Deploy to Databricks (main branch only)
4. **Run** -> Trigger DLT Pipeline

### Required Secrets

- `DATABRICKS_TOKEN`: Personal Access Token

## License

MIT License
