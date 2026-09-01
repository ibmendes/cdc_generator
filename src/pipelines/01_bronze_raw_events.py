"""
Bronze layer — raw ingestion of the fake CDC feed.

Reads the JSON batches dropped by `generate_cdc_batch.py` into the landing
volume, using Auto Loader (cloudFiles) for incremental, exactly-once file
discovery. Only the most basic structural expectations live here — anything
about the business meaning of the data (valid email, valid country, etc.)
belongs in the silver layer, closer to where it's used.

Uses the current `pyspark.pipelines` API (the module that replaced `dlt`).
If your workspace/runtime doesn't have this module yet, replace the import
below with `import dlt as dp` — the decorator names are identical.
"""

from pyspark import pipelines as dp

landing_path = spark.conf.get("landing_volume_path")


@dp.table(
    name="raw_customer_events",
    comment="Raw CDC events for the customers source table, ingested via Auto Loader.",
    table_properties={"quality": "bronze"},
)
@dp.expect_or_drop("has_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("has_valid_operation_type", "operation_type IN ('INSERT', 'UPDATE', 'DELETE')")
@dp.expect_or_drop("has_sequence_num", "sequence_num IS NOT NULL")
def raw_customer_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{landing_path}/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(landing_path)
    )
