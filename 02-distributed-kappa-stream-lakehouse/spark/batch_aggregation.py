"""
Batch aggregation job — reads the Delta Lake telemetry table and prints
four aggregations to stdout (visible in Airflow task logs).

Run manually inside the Spark container:
    docker exec telemetry-spark spark-submit \
      --packages io.delta:delta-spark_2.12:3.1.0 \
      /opt/spark-jobs/batch_aggregation.py

Or triggered automatically by the Airflow DAG (dags/telemetry_batch_dag.py).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
from pyspark.sql.functions import sum as _sum

DELTA_TABLE_PATH = "/opt/spark-jobs/delta/telemetry_events"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("TelemetryBatchAggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(DELTA_TABLE_PATH)
    total = df.count()

    print(f"\n{'=' * 52}")
    print(f"  Total events in Delta table: {total:,}")
    print(f"{'=' * 52}\n")

    print("── Events by type ──────────────────────────────")
    (
        df.groupBy("eventType")
        .agg(count("*").alias("event_count"))
        .orderBy(desc("event_count"))
        .show(truncate=False)
    )

    print("── Top 10 players by cumulative score ──────────")
    (
        df.groupBy("playerId")
        .agg(_sum("currentScore").alias("total_score"))
        .orderBy(desc("total_score"))
        .limit(10)
        .show(truncate=False)
    )

    print("── Events per zone ─────────────────────────────")
    (
        df.groupBy("zone")
        .agg(count("*").alias("event_count"))
        .orderBy(desc("event_count"))
        .show(truncate=False)
    )

    print("── Rings collected by character ────────────────")
    (
        df.groupBy("character")
        .agg(_sum("ringsCollected").alias("total_rings"))
        .orderBy(desc("total_rings"))
        .show(truncate=False)
    )


if __name__ == "__main__":
    main()
