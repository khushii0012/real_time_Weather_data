"""
Real-Time Weather Data Pipeline - Kafka Producer
================================================
Fetches real-time weather observations from the Open-Meteo REST API,
validates and formats the payload into a structured JSON event,
and publishes it to an Apache Kafka topic.
"""

import os
import sys
import time
import json
import uuid
import signal
import logging
from datetime import datetime, timezone
from pathlib import Path
import requests
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(dotenv_path=None):
        if dotenv_path and os.path.exists(dotenv_path):
            with open(dotenv_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip())

# Try importing KafkaProducer from kafka-python-ng or kafka-python
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    KafkaProducer = None
    KafkaError = None

# ---------------------------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)d) - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("WeatherProducer")

# ---------------------------------------------------------------------------
# Load Configuration from .env
# ---------------------------------------------------------------------------
# Look for .env in project root or config directory
root_dir = Path(__file__).resolve().parent.parent
env_paths = [root_dir / "config" / ".env", root_dir / ".env"]
for env_path in env_paths:
    if env_path.is_file():
        load_dotenv(dotenv_path=env_path)
        logger.info(f"Loaded configuration from: {env_path}")
        break

# API Settings
OPEN_METEO_BASE_URL = os.getenv("OPEN_METEO_BASE_URL", "https://api.open-meteo.com/v1/forecast")
WEATHER_CITY = os.getenv("WEATHER_CITY", "London")
WEATHER_LATITUDE = float(os.getenv("WEATHER_LATITUDE", "51.5074"))
WEATHER_LONGITUDE = float(os.getenv("WEATHER_LONGITUDE", "-0.1278"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "10"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "weather-data")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "weather-producer-client")

# Graceful Shutdown Flag
running = True

def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received (SIGINT/SIGTERM). Stopping producer gracefully...")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ---------------------------------------------------------------------------
# Kafka Producer Factory
# ---------------------------------------------------------------------------
def create_kafka_producer(bootstrap_servers: str, client_id: str) -> KafkaProducer:
    """
    Initializes a resilient Kafka Producer with retry and delivery acknowledgement settings.
    """
    if KafkaProducer is None:
        raise RuntimeError("kafka package is not installed. Please run: pip install kafka-python-ng")

    logger.info(f"Connecting to Kafka brokers at: {bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        client_id=client_id,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Producer reliability configurations
        acks="all",             # Wait for full in-sync replica acknowledgement
        retries=5,              # Automatically retry transient network failures
        retry_backoff_ms=500,   # Backoff between retry attempts
        request_timeout_ms=15000,
        max_block_ms=10000
    )
    logger.info("Kafka Producer successfully initialized.")
    return producer

# ---------------------------------------------------------------------------
# Open-Meteo API Fetcher
# ---------------------------------------------------------------------------
def fetch_weather_data(latitude: float, longitude: float, timeout: int = 10) -> dict:
    """
    Performs an HTTP GET request to the Open-Meteo REST API.
    Note: Open-Meteo does not push events; this client periodically polls the API.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation",
            "surface_pressure",
            "wind_speed_10m"
        ],
        "timezone": "UTC"
    }
    
    response = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=timeout)
    response.raise_for_status()  # Raise HTTPError for 4xx/5xx responses
    return response.json()

# ---------------------------------------------------------------------------
# Data Transformation and Validation
# ---------------------------------------------------------------------------
def transform_weather_event(raw_data: dict, city: str, latitude: float, longitude: float) -> dict:
    """
    Validates the raw API response and structures it into a standard JSON event schema.
    Appends a unique event_id (UUID4) and ingestion_timestamp.
    """
    if not isinstance(raw_data, dict):
        raise ValueError("Raw payload is not a valid JSON dictionary")

    current = raw_data.get("current")
    if not current or not isinstance(current, dict):
        raise ValueError("Missing or invalid 'current' block in Open-Meteo payload")

    # Extract required metrics
    obs_time = current.get("time")
    temp = current.get("temperature_2m")
    humidity = current.get("relative_humidity_2m")

    # Data validation checks
    if obs_time is None:
        raise ValueError("Observation timestamp ('time') is missing")
    if temp is None:
        raise ValueError("Temperature reading ('temperature_2m') is missing")
    if humidity is None:
        raise ValueError("Relative humidity reading ('relative_humidity_2m') is missing")

    # Sanity bounds check
    if not (-90.0 <= float(temp) <= 60.0):
        logger.warning(f"Extreme or outlier temperature detected: {temp}°C")
    if not (0.0 <= float(humidity) <= 100.0):
        logger.warning(f"Invalid humidity percentage detected: {humidity}%")

    # Standardized Event Schema
    event = {
        "event_id": str(uuid.uuid4()),
        "city": city,
        "latitude": float(latitude),
        "longitude": float(longitude),
        "temperature": float(temp),
        "relative_humidity": float(humidity),
        "apparent_temperature": float(current.get("apparent_temperature", temp)),
        "precipitation": float(current.get("precipitation", 0.0)),
        "surface_pressure": float(current.get("surface_pressure", 1013.25)),
        "wind_speed": float(current.get("wind_speed_10m", 0.0)),
        "observation_time": obs_time,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
    }
    return event

# ---------------------------------------------------------------------------
# Main Execution Loop
# ---------------------------------------------------------------------------
def run_producer(dry_run: bool = False):
    """
    Continuous loop: Polls Open-Meteo -> Transforms -> Sends to Kafka -> Sleeps.
    If dry_run is True, prints payload to stdout without publishing to Kafka.
    """
    producer = None
    if not dry_run:
        try:
            producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_CLIENT_ID)
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}. If Kafka is not running, test with --dry-run")
            return

    logger.info(f"Starting weather ingestion for '{WEATHER_CITY}' ({WEATHER_LATITUDE}, {WEATHER_LONGITUDE})")
    logger.info(f"Target Kafka Topic: '{KAFKA_TOPIC_NAME}' | Polling Interval: {POLL_INTERVAL_SECONDS}s")

    while running:
        try:
            logger.info("Fetching observation from Open-Meteo API...")
            raw_payload = fetch_weather_data(WEATHER_LATITUDE, WEATHER_LONGITUDE, REQUEST_TIMEOUT_SECONDS)
            event = transform_weather_event(raw_payload, WEATHER_CITY, WEATHER_LATITUDE, WEATHER_LONGITUDE)
            
            payload_json = json.dumps(event, indent=2)
            logger.info(f"Generated Weather Event:\n{payload_json}")

            if dry_run or producer is None:
                logger.info("[DRY RUN] Event successfully generated and validated (Kafka send skipped).")
            else:
                # Use city as partition key to ensure order per city
                future = producer.send(
                    topic=KAFKA_TOPIC_NAME,
                    key=event["city"],
                    value=event
                )
                # Block for synchronous receipt verification
                record_metadata = future.get(timeout=10)
                logger.info(
                    f"[PUBLISHED] Topic: {record_metadata.topic} | "
                    f"Partition: {record_metadata.partition} | "
                    f"Offset: {record_metadata.offset}"
                )

        except requests.exceptions.Timeout:
            logger.error(f"Open-Meteo API timed out after {REQUEST_TIMEOUT_SECONDS}s. Will retry next interval.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error communicating with Open-Meteo: {e}")
        except ValueError as e:
            logger.error(f"Payload validation failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected producer error: {e}", exc_info=True)

        # Sleep in small increments to respond quickly to shutdown signals
        for _ in range(POLL_INTERVAL_SECONDS):
            if not running:
                break
            time.sleep(1)

    if producer:
        logger.info("Flushing pending messages and closing Kafka Producer...")
        producer.flush(timeout=5)
        producer.close(timeout=5)
    logger.info("Weather Producer stopped cleanly.")

if __name__ == "__main__":
    is_dry_run = "--dry-run" in sys.argv
    run_producer(dry_run=is_dry_run)
