"""
Silver layer — data quality gate + CDC application (SCD Type 2).

Two steps:
  1. `customers_cdc_staging`: a view over the bronze table that carries the
     three expectation severities the exam covers:
       - expect              -> log the violation, keep the row   (warn)
       - expect_or_drop      -> drop the row, keep the flow going (drop)
       - expect_or_fail      -> stop the whole pipeline update    (fail)
  2. `create_auto_cdc_flow` applies the validated changes onto the
     `customers` streaming table, tracking full history (SCD Type 2) keyed
     by `customer_id` and ordered by `sequence_num`.

`create_auto_cdc_flow` replaced the older `apply_changes` function — same
signature, new name.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col

VALID_COUNTRIES = ("BR", "US", "PT", "DE", "FR", "AR", "MX", "CA")

dp.create_streaming_table(
    name="customers",
    comment="Customers dimension with full change history (SCD Type 2), built via AUTO CDC.",
    table_properties={"quality": "silver"},
)


@dp.view(
    name="customers_cdc_staging",
    comment="Validated CDC events, ready to be applied to the customers SCD2 table.",
)
# WARN only: log customers with a suspicious email but don't lose the change.
@dp.expect("valid_email_format", r"email RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'")
# DROP the row: an unknown/typo'd country code shouldn't block everything else.
@dp.expect_or_drop("known_country_code", f"country_code IN {VALID_COUNTRIES}")
# FAIL the pipeline: if we ever get a change without its ordering key, that's
# a generator/ingestion bug that needs a human, not a silently corrupted table.
@dp.expect_or_fail("has_ordering_key", "customer_id IS NOT NULL AND sequence_num IS NOT NULL")
def customers_cdc_staging():
    return dp.read_stream("raw_customer_events")


dp.create_auto_cdc_flow(
    target="customers",
    source="customers_cdc_staging",
    keys=["customer_id"],
    sequence_by=col("sequence_num"),
    apply_as_deletes="operation_type = 'DELETE'",
    except_column_list=["operation_type", "sequence_num", "event_timestamp"],
    stored_as_scd_type=2,
)
