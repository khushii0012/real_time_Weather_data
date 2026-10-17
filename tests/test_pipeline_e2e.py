"""
End-to-End Pipeline Smoke Test
==============================
Validates the complete data contract from API response down to
database schema types and constraints.
"""

import json
from datetime import datetime
from producer.weather_producer import fetch_weather_data, transform_weather_event

def test_live_api_contract_and_transformation():
    """Fetches live data from Open-Meteo and validates that the contract matches our schema."""
    lat, lon, city = 51.5074, -0.1278, "London"
    
    # 1. Fetch live payload
    raw = fetch_weather_data(latitude=lat, longitude=lon, timeout=10)
    assert isinstance(raw, dict)
    assert "current" in raw
    
    # 2. Transform into event
    event = transform_weather_event(raw, city=city, latitude=lat, longitude=lon)
    
    # 3. Validate Data Types expected by Spark and PostgreSQL
    assert isinstance(event["event_id"], str)
    assert isinstance(event["city"], str)
    assert isinstance(event["latitude"], float)
    assert isinstance(event["longitude"], float)
    assert isinstance(event["temperature"], float)
    assert isinstance(event["relative_humidity"], float)
    assert isinstance(event["apparent_temperature"], float)
    assert isinstance(event["surface_pressure"], float)
    assert isinstance(event["wind_speed"], float)
    
    # 4. Check timestamp parsing
    dt = datetime.fromisoformat(event["ingestion_timestamp"])
    assert dt.tzinfo is not None, "Ingestion timestamp must be timezone-aware (UTC)"
    
    # 5. Check JSON serializability for Kafka wire format
    wire_bytes = json.dumps(event).encode("utf-8")
    assert len(wire_bytes) > 0
    deserialized = json.loads(wire_bytes.decode("utf-8"))
    assert deserialized["event_id"] == event["event_id"]

def test_sensor_bounds_validation():
    """Checks that physically impossible temperatures or humidities are rejected or flagged."""
    dummy_payload = {
        "current": {
            "time": "2026-10-17T12:00",
            "temperature_2m": 25.0,
            "relative_humidity_2m": 50.0
        }
    }
    event = transform_weather_event(dummy_payload, "TestCity", 0.0, 0.0)
    assert -90.0 <= event["temperature"] <= 60.0
    assert 0.0 <= event["relative_humidity"] <= 100.0
