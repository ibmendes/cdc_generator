"""
Housekeeping task: compacts small files and cleans up old versions on the
CDC-managed silver table. This is the "other things" step of the job — a
good place to also plug in e.g. a Slack/webhook notification or a
Databricks SQL alert refresh once you want to extend the project further.

NOTE: Lakeflow Declarative Pipelines materialized views (like the gold
`customer_metrics_by_country_segment`) are auto-maintained by the pipeline
engine itself — you can't run OPTIMIZE/VACUUM on them manually (they're
registered as views, not tables). Point this at a real streaming table
instead, such as the silver `customers` SCD2 table, which accumulates a lot
of small files from continuous CDC upserts.
"""

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optimize and vacuum a pipeline-managed streaming table")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", default="customers")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    full_table_name = f"{args.catalog}.{args.schema}.{args.table}"

    print(f"Running OPTIMIZE on {full_table_name}")
    spark.sql(f"OPTIMIZE {full_table_name}")

    print(f"Running VACUUM on {full_table_name} (default retention)")
    spark.sql(f"VACUUM {full_table_name}")

    print("Housekeeping complete.")


if __name__ == "__main__":
    main()