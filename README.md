# cdc-lakeflow-cert-lab

Lab project for the Databricks **Data Engineer Professional** certification,
covering: CDC, Lakeflow Declarative Pipelines (LSDP), Lakeflow Jobs, Unity
Catalog resources, and Databricks Asset Bundles (DABs) deployed via the CLI.

## Architecture

```
generate_cdc_batch.py (job task, serverless)
        │  writes JSON batches (INSERT/UPDATE/DELETE)
        ▼
/Volumes/<catalog>/<schema>/cdc_landing/landing/
        │  Auto Loader
        ▼
[Lakeflow Declarative Pipeline]
  bronze: raw_customer_events            (expect_or_drop on structure)
  silver: customers_cdc_staging (view)   (expect / expect_or_drop / expect_or_fail)
       └─ create_auto_cdc_flow ─▶ customers  (SCD Type 2)
  gold:   customer_metrics_by_country_segment (materialized view)
        │
        ▼
check_expectations_log.py (job task)  -> dq_report table
optimize_gold_table.py (job task)     -> OPTIMIZE + VACUUM
```

All compute is **serverless** (pipeline and every job task).


## Deploying and running (CLI)

```bash
# sanity check the bundle config resolves correctly
databricks bundle validate -t dev

# deploy schema + volume + pipeline + job to your dev workspace
databricks bundle deploy -t dev

# run the whole orchestration job once, end to end
databricks bundle run cdc_orchestration_job -t dev

# run again with a bigger batch, overriding the job parameter
databricks bundle run cdc_orchestration_job -t dev --params batch_size=200

# check what would change before promoting
databricks bundle validate -t staging
databricks bundle deploy -t staging

# tear everything down from a given target when you're done experimenting
databricks bundle destroy -t dev
```

## Local unit tests (no workspace needed)

```bash
pip install -e ".[dev]"
pytest
```