# Pipeline Orchestration Guide

## Architecture Overview

This project implements a **Medallion Architecture** for customer data processing with two orchestration options:

### Current Implementation: DLT Pipeline (Recommended)

```
Source Table (workspace.customer.customer_raw_source)
                    |
                    v
    ┌───────────────────────────────────┐
    │   DLT Pipeline (Single Unit)      │
    │                                   │
    │   Bronze Layer                    │
    │   └─> bronze_customer             │
    │          |                        │
    │          v                        │
    │   Silver Layer                    │
    │   ├─> silver_customer             │
    │   └─> silver_customer_quarantine  │
    │          |                        │
    │          +─────────┬──────────+   │
    │          v         v          v   │
    │   Gold Layer                      │
    │   ├─> gold_customer_current       │
    │   ├─> gold_customer_history       │
    │   └─> gold_customer_by_state      │
    └───────────────────────────────────┘

Dependencies handled automatically by DLT
```

**Key Features:**
* **Automatic dependency resolution** - DLT reads determine order
* **Built-in data quality** - Expectations and quarantine tables
* **Incremental processing** - Only processes new/changed data
* **Unified execution** - Single pipeline run for all layers
* **Change data capture** - Automatic SCD Type 2 support

---

## Option 1: DLT Pipeline with Job Trigger (Implemented)

### File: `resources/job.yml`

**Job Structure:**
```yaml
Job: customer_medallion_pipeline
  └─ Task: run_medallion_pipeline
       └─ Type: pipeline_task
       └─ Pipeline ID: 566387c8-eb18-469b-8f6d-808f90c0e63d
       └─ Schedule: Daily at 2 AM
```

**Advantages:**
* ✅ Simple - Single task triggers entire pipeline
* ✅ Uses existing DLT notebooks
* ✅ Automatic dependency management
* ✅ Built-in data quality and expectations
* ✅ Incremental processing out of the box
* ✅ Better performance (shared cluster across all layers)

**Deployment:**
```bash
# Navigate to project directory
cd customer-dlt-project

# Validate the bundle
databricks bundle validate

# Deploy to development
databricks bundle deploy -t development

# Run the job manually
databricks bundle run customer_medallion_pipeline -t development
```

---

## Option 2: Multi-Task Notebook Job (Alternative)

### File: `resources/job_multitask_alternative.yml`

**Job Structure:**
```
Job: customer_medallion_multitask
  |
  └─ bronze_task
       |
       └─ silver_task
            |
            ├─ gold_type1_task
            |       |
            ├─ gold_type2_task
            |       |
            └───┴───> country_aggregation_task
```

**Dependency Flow:**
```
bronze_task
    ↓
silver_task
    ↓
    ├──> gold_type1_task ──┐
    └──> gold_type2_task ──┤
                           ↓
              country_aggregation_task
```

**Requirements:**
* ⚠️ Requires **regular PySpark notebooks** (not DLT notebooks)
* ⚠️ Current notebooks use `@dlt.table` decorators and won't work
* ⚠️ Would need to rewrite all notebooks as standard Spark code

**To Use This Approach:**
1. Convert DLT notebooks to regular PySpark notebooks
2. Replace `@dlt.table` with standard DataFrame operations
3. Replace `dlt.read()` with `spark.table()` or `spark.read`
4. Add explicit writes: `df.write.mode("overwrite").saveAsTable()`
5. Use `job_multitask_alternative.yml` instead of `job.yml`

---

## Comparison

| Feature | DLT Pipeline (Option 1) | Multi-Task Job (Option 2) |
|---------|------------------------|---------------------------|
| **Setup Complexity** | Simple | Complex |
| **Dependency Management** | Automatic | Manual |
| **Data Quality** | Built-in | Manual implementation |
| **Incremental Processing** | Built-in | Manual implementation |
| **Performance** | Better (shared cluster) | Separate clusters per task |
| **Cost** | Lower | Higher |
| **Debugging** | Pipeline UI + logs | Per-task logs |
| **Current Status** | ✅ Implemented | ⚠️ Requires rewrite |

---

## Deployment Instructions

### Prerequisites
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token
```

### Deploy DLT Pipeline Job

```bash
# Navigate to project
cd customer-dlt-project

# Validate configuration
databricks bundle validate

# Deploy to development
databricks bundle deploy -t development

# The deployment will create:
# 1. DLT Pipeline: 566387c8-eb18-469b-8f6d-808f90c0e63d
# 2. Job: customer_medallion_pipeline

# Run manually (on-demand)
databricks bundle run customer_medallion_pipeline -t development

# Or trigger via UI
# Workflows -> customer_medallion_pipeline -> Run Now
```

### Monitor Pipeline Execution

```bash
# View pipeline runs
databricks pipelines list-updates --pipeline-id 566387c8-eb18-469b-8f6d-808f90c0e63d

# View job runs
databricks jobs list-runs --job-name customer_medallion_pipeline

# View pipeline in UI
# Workflows -> DLT -> New Pipeline 2026-09-17 14:04
```

---

## Schedule Configuration

The job is configured to run **daily at 2 AM**:

```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: "America/Los_Angeles"
  pause_status: "UNPAUSED"
```

**To change schedule:**
```yaml
# Hourly
quartz_cron_expression: "0 0 * * * ?"

# Every 6 hours
quartz_cron_expression: "0 0 */6 * * ?"

# Monday-Friday at 8 AM
quartz_cron_expression: "0 0 8 ? * MON-FRI"
```

---

## Parameters

The job accepts runtime parameters:

```bash
# Full refresh (reprocess all data)
databricks jobs run-now --job-id <job-id> \
  --notebook-params '{"full_refresh": "true"}'

# Refresh specific tables
databricks jobs run-now --job-id <job-id> \
  --notebook-params '{"refresh_selection": "bronze_customer,silver_customer"}'
```

---

## CI/CD Integration

### GitHub Actions

```yaml
name: Deploy DLT Pipeline
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install Databricks CLI
        run: pip install databricks-cli
      
      - name: Deploy Bundle
        env:
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
        run: |
          cd customer-dlt-project
          databricks bundle deploy -t production
```

---

## Troubleshooting

### Issue: Job fails with "Pipeline not found"
**Solution:** Ensure pipeline ID in `job.yml` matches your actual pipeline ID

### Issue: DLT notebooks fail when run as regular notebook tasks
**Solution:** DLT notebooks must be run through a DLT pipeline, not as regular notebook tasks

### Issue: Tables not created
**Solution:** Check pipeline execution logs in DLT UI for errors

### Issue: Incremental processing not working
**Solution:** Ensure source data has proper append semantics or use full_refresh=true

---

## Next Steps

1. ✅ DLT Pipeline created
2. ✅ Job configuration created
3. ⏳ Deploy the bundle
4. ⏳ Run the pipeline
5. ⏳ Verify tables in catalog
6. ⏳ Set up monitoring and alerts

---

## Recommendation

**Use Option 1 (DLT Pipeline)** - It's simpler, more maintainable, and provides better out-of-the-box features for data engineering workflows.

Only use Option 2 if you have specific requirements that DLT cannot fulfill (e.g., custom Python libraries, complex business logic that doesn't fit DLT patterns).
