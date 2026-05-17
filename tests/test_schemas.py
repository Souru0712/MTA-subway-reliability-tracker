"""
Validate that TRIP_UPDATE_SCHEMA and VEHICLE_POSITION_SCHEMA field names match
the JSON keys the producer emits. No Spark session needed — checks definitions only.
"""
from src.transform.schemas import ALERTS_SCHEMA, TRIP_UPDATE_SCHEMA, VEHICLE_POSITION_SCHEMA

EXPECTED_TRIP_UPDATE_FIELDS = {
    "event_time",
    "feed_id",
    "trip_id",
    "route_id",
    "stop_id",
    "arrival_time",
    "departure_time",
    "delay_seconds",
}

EXPECTED_VEHICLE_POSITION_FIELDS = {
    "event_time",
    "feed_id",
    "trip_id",
    "route_id",
    "stop_id",
    "current_status",
    "latitude",
    "longitude",
    "timestamp",
}


def test_trip_update_schema_fields():
    actual = {f.name for f in TRIP_UPDATE_SCHEMA.fields}
    assert actual == EXPECTED_TRIP_UPDATE_FIELDS


def test_vehicle_position_schema_fields():
    actual = {f.name for f in VEHICLE_POSITION_SCHEMA.fields}
    assert actual == EXPECTED_VEHICLE_POSITION_FIELDS


def test_all_trip_update_fields_nullable():
    for field in TRIP_UPDATE_SCHEMA.fields:
        assert field.nullable, f"Field {field.name} should be nullable"


def test_all_vehicle_position_fields_nullable():
    for field in VEHICLE_POSITION_SCHEMA.fields:
        assert field.nullable, f"Field {field.name} should be nullable"


def test_no_extra_trip_update_fields():
    actual = {f.name for f in TRIP_UPDATE_SCHEMA.fields}
    assert actual == EXPECTED_TRIP_UPDATE_FIELDS, (
        f"Unexpected fields: {actual - EXPECTED_TRIP_UPDATE_FIELDS}, "
        f"missing fields: {EXPECTED_TRIP_UPDATE_FIELDS - actual}"
    )


def test_no_extra_vehicle_position_fields():
    actual = {f.name for f in VEHICLE_POSITION_SCHEMA.fields}
    assert actual == EXPECTED_VEHICLE_POSITION_FIELDS, (
        f"Unexpected fields: {actual - EXPECTED_VEHICLE_POSITION_FIELDS}, "
        f"missing fields: {EXPECTED_VEHICLE_POSITION_FIELDS - actual}"
    )


EXPECTED_ALERTS_FIELDS = {
    "event_time",
    "feed_id",
    "alert_id",
    "route_id",
    "stop_id",
    "cause",
    "effect",
    "header",
    "active_period_start",
    "active_period_end",
}


def test_alerts_schema_fields():
    actual = {f.name for f in ALERTS_SCHEMA.fields}
    assert actual == EXPECTED_ALERTS_FIELDS


def test_all_alerts_fields_nullable():
    for field in ALERTS_SCHEMA.fields:
        assert field.nullable, f"Field {field.name} should be nullable"


def test_no_extra_alerts_fields():
    actual = {f.name for f in ALERTS_SCHEMA.fields}
    assert actual == EXPECTED_ALERTS_FIELDS, (
        f"Unexpected fields: {actual - EXPECTED_ALERTS_FIELDS}, "
        f"missing fields: {EXPECTED_ALERTS_FIELDS - actual}"
    )
