"""
Tests for GTFS protobuf parsing without a live Kafka broker.
Uses the gtfs_realtime_pb2 library directly to build fixture messages.
"""
import json
from unittest.mock import MagicMock

import pytest
from google.transit import gtfs_realtime_pb2

from src.ingestion.gtfs_producer import _feed_id, _parse_trip_updates, _parse_vehicle_positions


def _make_trip_feed(route_id: str, stop_id: str, delay: int) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "test-entity-1"
    tu = entity.trip_update
    tu.trip.trip_id = "trip-001"
    tu.trip.route_id = route_id
    stu = tu.stop_time_update.add()
    stu.stop_id = stop_id
    stu.arrival.time = 1745836800
    stu.arrival.delay = delay
    return feed


def _make_vehicle_feed(route_id: str, stop_id: str) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "vp-entity-1"
    vp = entity.vehicle
    vp.trip.trip_id = "trip-001"
    vp.trip.route_id = route_id
    vp.stop_id = stop_id
    vp.current_status = gtfs_realtime_pb2.VehiclePosition.IN_TRANSIT_TO
    vp.position.latitude = 40.7128
    vp.position.longitude = -74.0060
    vp.timestamp = 1745836800
    return feed


def test_parse_trip_updates_produces_correct_shape():
    feed = _make_trip_feed("4", "640N", 120)
    produced = []

    mock_producer = MagicMock()
    mock_producer.produce.side_effect = lambda topic, key, value, callback: produced.append(
        json.loads(value)
    )

    count = _parse_trip_updates(feed, "gtfs", mock_producer, "mta.trip_updates")

    assert count == 1
    msg = produced[0]
    assert msg["route_id"] == "4"
    assert msg["stop_id"] == "640N"
    assert msg["delay_seconds"] == 120
    assert "event_time" in msg
    assert "feed_id" in msg


def test_parse_trip_updates_skips_non_trip_update():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "alert-1"
    alert = entity.alert
    alert.header_text.translation.add().text = "Delays on A line"

    mock_producer = MagicMock()
    count = _parse_trip_updates(feed, "gtfs", mock_producer, "mta.trip_updates")
    assert count == 0
    mock_producer.produce.assert_not_called()


def test_parse_trip_updates_empty_feed():
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"

    mock_producer = MagicMock()
    count = _parse_trip_updates(feed, "gtfs", mock_producer, "mta.trip_updates")
    assert count == 0
    mock_producer.produce.assert_not_called()


def test_parse_trip_updates_no_arrival_field():
    # departure-only stop time update — delay_seconds should be None
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "test-entity-2"
    tu = entity.trip_update
    tu.trip.trip_id = "trip-002"
    tu.trip.route_id = "A"
    stu = tu.stop_time_update.add()
    stu.stop_id = "A27N"
    stu.departure.time = 1745836800
    # no arrival set

    produced = []
    mock_producer = MagicMock()
    mock_producer.produce.side_effect = lambda topic, key, value, callback: produced.append(
        json.loads(value)
    )

    count = _parse_trip_updates(feed, "gtfs", mock_producer, "mta.trip_updates")
    assert count == 1
    assert produced[0]["delay_seconds"] is None
    assert produced[0]["arrival_time"] is None


def test_parse_vehicle_positions_produces_correct_shape():
    feed = _make_vehicle_feed("A", "A27N")
    produced = []

    mock_producer = MagicMock()
    mock_producer.produce.side_effect = lambda topic, key, value, callback: produced.append(
        json.loads(value)
    )

    count = _parse_vehicle_positions(feed, "gtfs", mock_producer, "mta.vehicle_positions")

    assert count == 1
    msg = produced[0]
    assert msg["route_id"] == "A"
    assert msg["stop_id"] == "A27N"
    assert msg["latitude"] == pytest.approx(40.7128, abs=0.001)
    assert msg["longitude"] == pytest.approx(-74.0060, abs=0.001)
    assert "current_status" in msg
    assert "event_time" in msg


def test_feed_id_extraction():
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace"
    assert _feed_id(url) == "gtfs-ace"

    url2 = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
    assert _feed_id(url2) == "gtfs"
