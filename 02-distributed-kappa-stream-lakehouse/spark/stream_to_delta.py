"""
Component 3 — Spark Structured Streaming consumer.

Reads the immutable "telemetry-events" Kafka log in micro-batches, enforces
a strict schema on the JSON payload (malformed events are dropped rather than
crashing the job), and writes the result as an ACID Delta Lake table.

Checkpointing to /opt/spark-jobs/checkpoints gives exactly-once write
semantics across restarts: Spark resumes from the last committed Kafka
offset rather than re-reading or skipping events.

Run inside the Spark container (see CLAUDE.md for the full spark-submit
command and required Delta/Kafka packages):

    docker exec -it telemetry-spark spark-submit \
      --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      /opt/spark-jobs/stream_to_delta.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

KAFKA_BROKER = "telemetry-kafka:9092"  # in-network listener, not the host-facing 9094
KAFKA_TOPIC = "telemetry-events"
DELTA_TABLE_PATH = "/opt/spark-jobs/delta/telemetry_events"
CHECKPOINT_PATH = "/opt/spark-jobs/checkpoints/telemetry_events"

TELEMETRY_SCHEMA = StructType(
    [
        StructField("playerId", StringType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("gameTitle", StringType(), nullable=True),
        StructField("character", StringType(), nullable=True),
        StructField("zone", StringType(), nullable=True),
        StructField("ringsCollected", IntegerType(), nullable=True),
        StructField("currentScore", IntegerType(), nullable=True),
        StructField("eventType", StringType(), nullable=False),
    ]
)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("TelemetryKappaStreamToDelta")
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

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka's "value" column is raw bytes — decode to string, parse as JSON
    # against TELEMETRY_SCHEMA, then drop rows that failed to parse
    # (from_json yields null on a schema mismatch instead of raising).
    events = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), TELEMETRY_SCHEMA).alias("event"))
        .select("event.*")
        .where(col("playerId").isNotNull() & col("eventType").isNotNull())
    )

    query = (
        events.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(DELTA_TABLE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
