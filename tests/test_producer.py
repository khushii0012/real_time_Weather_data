"""
Unit Tests for Weather Producer
===============================
Tests API response transformations, schema integrity, and error handling.
"""

import pytest
from producer.weather_producer import transform_weather_event

SAMPLE_OPEN_METEO_PAYLOAD = {
    "latitude": 51.5,
    "longitude": -0.12,
    "current": {
        "time": "2026-10-17T13:45",
        "interval": 900,
        "temperature_2m": 15.2,
        "relative_humidity_2m": 62.0,
        "apparent_temperature": 14.0,
        "precipitation": 0.0,
        "surface_pressure": 1018.4,
        "wind_speed_10m": 8.5
    }
}

def test_transform_weather_event_success():
    """Verify that a valid Open-Meteo payload is properly transformed into our event schema."""
    event = transform_weather_event(
        raw_data=SAMPLE_OPEN_METEO_PAYLOAD,
        city="London",
        latitude=51.5074,
        longitude=-0.1278
    )
    
    assert "event_id" in event
    assert len(event["event_id"]) == 36  # UUID4 length
    assert event["city"] == "London"
    assert event["latitude"] == 51.5074
    assert event["longitude"] == -0.1278
    assert event["temperature"] == 15.2
    assert event["relative_humidity"] == 62.0
    assert event["apparent_temperature"] == 14.0
    assert event["surface_pressure"] == 1018.4
    assert event["wind_speed"] == 8.5
    assert event["observation_time"] == "2026-10-17T13:45"
    assert "ingestion_timestamp" in event

def test_transform_missing_current_block():
    """Verify ValueError is raised if 'current' block is missing."""
    with pytest.raises(ValueError, match="Missing or invalid 'current' block"):
        transform_weather_event({"latitude": 51.5}, "London", 51.5, -0.12)

def test_transform_missing_temperature():
    """Verify ValueError is raised if temperature is missing."""
    bad_payload = {
        "current": {
            "time": "2026-10-17T13:45",
            "relative_humidity_2m": 60.0
        }
    }
    with pytest.raises(ValueError, match="Temperature reading.*missing"):
        transform_weather_event(bad_payload, "London", 51.5, -0.12)

def test_transform_missing_humidity():
    """Verify ValueError is raised if humidity is missing."""
    bad_payload = {
        "current": {
            "time": "2026-10-17T13:45",
            "temperature_2m": 22.0
        }
    }
    with pytest.raises(ValueError, match="Relative humidity.*missing"):
        transform_weather_event(bad_payload, "London", 51.5, -0.12)

def test_transform_missing_time():
    """Verify ValueError is raised if observation time is missing."""
    bad_payload = {
        "current": {
            "temperature_2m": 22.0,
            "relative_humidity_2m": 60.0
        }
    }
    with pytest.raises(ValueError, match="Observation timestamp.*missing"):
        transform_weather_event(bad_payload, "London", 51.5, -0.12)
