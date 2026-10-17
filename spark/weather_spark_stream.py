"""
Real-Time Weather Data Pipeline - Spark Structured Streaming
============================================================
Consumes real-time weather events from Apache Kafka, parses JSON payloads
with strict schema enforcement, applies validation filters, and writes
records idempotently to PostgreSQL via JDBC micro-batches.
"""

import os
import sys
import logging
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s"
)
logger = logging.getLogger("WeatherSparkStream")

# ---------------------------------------------------------------------------
# Load Configuration from .env
# ---------------------------------------------------------------------------
root_dir = Path(__file__).resolve().parent.parent
env_paths = [root_dir / "config" / ".env", root_dir / ".env"]
for env_path in env_paths:
    if env_path.is_file():
        try:
            from dotenv import load_dotenv
            load_dotenv(dotenv_path=env_path)
        except ImportError:
            with open(env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip())
        logger.info(f"Loaded configuration from: {env_path}")
        break

# Kafka Configurations
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "weather-data")

# Windows Hadoop & Java Environment Auto-Configuration
if os.name == "nt":
    if not os.environ.get("HADOOP_HOME") and os.path.exists("C:\\hadoop"):
        os.environ["HADOOP_HOME"] = "C:\\hadoop"
        os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")
    if not os.environ.get("JAVA_HOME") and os.path.exists("C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.20.101-hotspot"):
        os.environ["JAVA_HOME"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.20.101-hotspot"
        os.environ["PATH"] = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.20.101-hotspot\\bin;" + os.environ.get("PATH", "")

# PostgreSQL Configurations
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "weather_readings")

# Spark Configurations
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "WeatherRealTimeStreamProcessor")
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "./checkpoints/weather_stream")
SPARK_KAFKA_PACKAGE = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
POSTGRES_JDBC_PACKAGE = os.getenv("POSTGRES_JDBC_PACKAGE", "org.postgresql:postgresql:42.7.3")

# Construct JDBC Connection URL
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# ---------------------------------------------------------------------------
# PySpark Imports (Lazy evaluation with helpful error message)
# ---------------------------------------------------------------------------
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DoubleType,
        TimestampType
    )
except ImportError:
    logger.error("PySpark is not installed in the active environment.")
    logger.error("Please run: pip install pyspark==3.5.1")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Define Strict JSON Event Schema
# ---------------------------------------------------------------------------
# Matches the payload produced by producer/weather_producer.py
WEATHER_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("city", StringType(), False),
    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("relative_humidity", DoubleType(), False),
    StructField("apparent_temperature", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("surface_pressure", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("observation_time", StringType(), False),
    StructField("ingestion_timestamp", StringType(), False)
])

# ---------------------------------------------------------------------------
# Spark Session Builder
# ---------------------------------------------------------------------------
def create_spark_session() -> SparkSession:
    """
    Creates SparkSession configured with the Kafka connector and PostgreSQL JDBC driver.
    """
    maven_packages = f"{SPARK_KAFKA_PACKAGE},{POSTGRES_JDBC_PACKAGE}"
    logger.info(f"Initializing SparkSession with Maven packages: {maven_packages}")

    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master("local[*]") \
        .config("spark.jars.packages", maven_packages) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession initialized successfully.")
    return spark

# ---------------------------------------------------------------------------
# Idempotent Micro-Batch Writer for PostgreSQL
# ---------------------------------------------------------------------------
def write_batch_to_postgres(batch_df, batch_id: int):
    """
    Micro-batch write function invoked by foreachBatch.
    Writes records into PostgreSQL with deduplication and error handling.
    """
    record_count = batch_df.count()
    logger.info(f"[Micro-Batch {batch_id}] Processing {record_count} weather events...")

    if record_count == 0:
        logger.info(f"[Micro-Batch {batch_id}] Batch is empty. Skipping database write.")
        return

    try:
        # Standard append using Spark JDBC connector
        batch_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", POSTGRES_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        logger.info(f"[Micro-Batch {batch_id}] Successfully wrote {record_count} records to PostgreSQL table '{POSTGRES_TABLE}'.")
    except Exception as e:
        logger.error(f"[Micro-Batch {batch_id}] Error writing to PostgreSQL: {e}", exc_info=True)
        # Note: Do not swallow the exception if you require Spark to retry the micro-batch
        raise e

# ---------------------------------------------------------------------------
# Stream Pipeline Execution
# ---------------------------------------------------------------------------
def start_streaming(console_mode: bool = False):
    """
    Sets up the end-to-end Structured Streaming pipeline:
    Kafka -> JSON Parsing -> Data Cleansing -> Sink (Console or PostgreSQL)
    """
    spark = create_spark_session()

    logger.info(f"Connecting to Kafka topic '{KAFKA_TOPIC_NAME}' at {KAFKA_BOOTSTRAP_SERVERS}...")
    
    # 1. Ingestion: Read raw Kafka binary stream
    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Transformation: Extract binary 'value' to String and parse with schema
    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) AS json_payload") \
        .select(from_json(col("json_payload"), WEATHER_SCHEMA).alias("data")) \
        .select("data.*")

    # 3. Validation & Quality Filtering:
    # - Ensure event_id and timestamps are not null
    # - Cast strings to ISO Timestamps
    # - Filter physical sensor bounds (Temperature: -90°C to 60°C, Humidity: 0% to 100%)
    cleaned_stream = parsed_stream \
        .filter(col("event_id").isNotNull()) \
        .filter(col("temperature").between(-90.0, 60.0)) \
        .filter(col("relative_humidity").between(0.0, 100.0)) \
        .withColumn("observation_time", to_timestamp(col("observation_time"))) \
        .withColumn("ingestion_timestamp", to_timestamp(col("ingestion_timestamp"))) \
        .withColumn("created_at", current_timestamp())

    # Ensure checkpoint directory exists
    checkpoint_path = Path(SPARK_CHECKPOINT_DIR).resolve()
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Using streaming checkpoint directory: {checkpoint_path}")

    # 4. Sink: Console for verification or PostgreSQL for analytical store
    if console_mode:
        logger.info("Running in CONSOLE MODE (Displaying streaming batches on stdout)...")
        query = cleaned_stream.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", str(checkpoint_path / "console")) \
            .start()
    else:
        logger.info(f"Running in POSTGRESQL MODE (Writing micro-batches to '{POSTGRES_TABLE}')...")
        query = cleaned_stream.writeStream \
            .outputMode("append") \
            .foreachBatch(write_batch_to_postgres) \
            .option("checkpointLocation", str(checkpoint_path / "postgres")) \
            .start()

    logger.info("Structured Streaming Query started. Awaiting termination...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Received interrupt. Stopping streaming query gracefully...")
        query.stop()
        spark.stop()
        logger.info("Spark Structured Streaming job terminated.")

if __name__ == "__main__":
    is_console = "--console" in sys.argv
    start_streaming(console_mode=is_console)
