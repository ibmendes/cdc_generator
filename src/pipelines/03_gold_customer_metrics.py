"""
Gold layer — business-facing aggregate.

Materialized view counting currently-active customers per country/segment,
built from the *current* version of each customer in the SCD Type 2 table
(rows where `__END_AT IS NULL`).
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, current_timestamp


@dp.materialized_view(
    name="customer_metrics_by_country_segment",
    comment="Count of currently-active customers per country and segment.",
    table_properties={"quality": "gold"},
)
def customer_metrics_by_country_segment():
    return (
        dp.read("customers")
        .filter(col("__END_AT").isNull())
        .groupBy("country_code", "segment")
        .agg(count("*").alias("active_customers"))
        .withColumn("computed_at", current_timestamp())
    )
