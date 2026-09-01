"""
Reads the Lakeflow Declarative Pipeline event log (published as a UC table
via the `event_log` block in resources/pipeline.yml) and summarizes how many
records passed/failed each data-quality expectation in the most recent
pipeline update.

Run as a `spark_python_task` on serverless compute — a SparkSession is
already available as `spark`.

NOTE: the `:` path syntax (`details:flow_progress:data_quality`) is
semi-structured column access and is only understood when it goes through
the SQL parser (spark.sql / selectExpr / expr()). Passing a string like
that into col(...) does NOT work — col() treats the whole string as a
literal column name and analysis fails with UNRESOLVED_COLUMN. To avoid
mixing the two APIs by accident, this script does everything in one
spark.sql() query.
"""

import argparse

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize data-quality results from the pipeline event log")
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    return parser.parse_args()


EXPECTATIONS_SCHEMA = "array<struct<name:string,dataset:string,passed_records:long,failed_records:long>>"


def main() -> None:
    args = parse_args()
    event_log_table = f"{args.catalog}.{args.schema}.pipeline_event_log"
    dq_report_table = f"{args.catalog}.{args.schema}.dq_report"

    quality_events = spark.sql(f"""
        SELECT
            timestamp,
            flow_name,
            expectation.name AS expectation_name,
            expectation.dataset AS dataset,
            expectation.passed_records AS passed_records,
            expectation.failed_records AS failed_records
        FROM (
            SELECT
                timestamp,
                origin.flow_name AS flow_name,
                explode(
                    from_json(
                        CAST(details:flow_progress:data_quality:expectations AS STRING),
                        '{EXPECTATIONS_SCHEMA}'
                    )
                ) AS expectation
            FROM {event_log_table}
            WHERE origin.pipeline_id = '{args.pipeline_id}'
              AND event_type = 'flow_progress'
              AND details:flow_progress:data_quality IS NOT NULL
        )
    """)

    summary = (
        quality_events.groupBy("flow_name", "dataset", "expectation_name")
        .sum("passed_records", "failed_records")
        .withColumnRenamed("sum(passed_records)", "total_passed")
        .withColumnRenamed("sum(failed_records)", "total_failed")
    )

    rows = summary.collect()
    if not rows:
        print("No data-quality events found yet for this pipeline (first run?). Nothing to report.")
        return

    print("Data quality summary for this pipeline:")
    any_failures = False
    for row in rows:
        print(
            f"  [{row['dataset']}] {row['expectation_name']}: "
            f"passed={row['total_passed']} failed={row['total_failed']}"
        )
        if row["total_failed"] and row["total_failed"] > 0:
            any_failures = True

    # Persist the report as a small table too, so you can chart it over time.
    summary.write.mode("append").saveAsTable(dq_report_table)

    if any_failures:
        print("WARNING: at least one expectation had failed/dropped records in this run.")
        # Don't hard-fail the job here on purpose — expect_or_drop/expect are
        # meant to be non-fatal. expect_or_fail already stops the pipeline
        # task upstream if violated.


if __name__ == "__main__":
    main()