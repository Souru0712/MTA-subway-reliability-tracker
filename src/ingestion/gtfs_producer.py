import json
import logging
import time
from datetime import datetime, timezone

import requests
import urllib3
from confluent_kafka import Producer
from google.transit import gtfs_realtime_pb2

from config.settings import Config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


def _delivery_report(err, msg) -> None:
    if err:
        logger.error("Delivery failed for topic %s: %s", msg.topic(), err)


def _build_producer(cfg: Config) -> Producer:
    return Producer({"bootstrap.servers": cfg.kafka_bootstrap_servers})


def _fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage | None:
    try:
        # MTA's API doesn't serve its full certificate chain, so SSL verification
        # fails even with a valid CA bundle. Safe to skip — this is public read-only data.
        resp = requests.get(url, timeout=15, verify=False)
        resp.raise_for_status()
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(resp.content)
        return feed
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return None


def _feed_id(url: str) -> str:
    return url.rstrip("/").split("%2F")[-1].split("/")[-1]


def _parse_trip_updates(
    feed: gtfs_realtime_pb2.FeedMessage,
    feed_id: str,
    producer: Producer,
    topic: str,
) -> int:
    count = 0
    event_time = datetime.now(timezone.utc).isoformat()

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip_id = tu.trip.trip_id
        route_id = tu.trip.route_id

        for stu in tu.stop_time_update:
            msg = {
                "event_time": event_time,
                "feed_id": feed_id,
                "trip_id": trip_id,
                "route_id": route_id,
                "stop_id": stu.stop_id,
                "arrival_time": stu.arrival.time if stu.HasField("arrival") else None,
                "departure_time": stu.departure.time if stu.HasField("departure") else None,
                "delay_seconds": stu.arrival.delay if stu.HasField("arrival") else None,
            }
            producer.produce(
                topic,
                key=trip_id,
                value=json.dumps(msg),
                callback=_delivery_report,
            )
            count += 1

    return count


def _parse_vehicle_positions(
    feed: gtfs_realtime_pb2.FeedMessage,
    feed_id: str,
    producer: Producer,
    topic: str,
) -> int:
    count = 0
    event_time = datetime.now(timezone.utc).isoformat()

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vp = entity.vehicle
        msg = {
            "event_time": event_time,
            "feed_id": feed_id,
            "trip_id": vp.trip.trip_id,
            "route_id": vp.trip.route_id,
            "stop_id": vp.stop_id,
            "current_status": gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vp.current_status),
            "latitude": vp.position.latitude if vp.HasField("position") else None,
            "longitude": vp.position.longitude if vp.HasField("position") else None,
            "timestamp": vp.timestamp,
        }
        producer.produce(
            topic,
            key=vp.trip.trip_id,
            value=json.dumps(msg),
            callback=_delivery_report,
        )
        count += 1

    return count


def _parse_alerts(
    feed: gtfs_realtime_pb2.FeedMessage,
    feed_id: str,
    producer: Producer,
    topic: str,
) -> int:
    count = 0
    event_time = datetime.now(timezone.utc).isoformat()

    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue
        alert = entity.alert
        cause = gtfs_realtime_pb2.Alert.Cause.Name(alert.cause)
        effect = gtfs_realtime_pb2.Alert.Effect.Name(alert.effect)
        header = alert.header_text.translation[0].text if alert.header_text.translation else None
        active_start = alert.active_period[0].start if alert.active_period else None
        active_end = alert.active_period[0].end if alert.active_period else None

        # flatten by informed entity — one row per affected route/stop
        for ie in alert.informed_entity:
            msg = {
                "event_time": event_time,
                "feed_id": feed_id,
                "alert_id": entity.id,
                "route_id": ie.route_id or None,
                "stop_id": ie.stop_id or None,
                "cause": cause,
                "effect": effect,
                "header": header,
                "active_period_start": active_start,
                "active_period_end": active_end,
            }
            producer.produce(
                topic,
                key=entity.id,
                value=json.dumps(msg),
                callback=_delivery_report,
            )
            count += 1

    return count


def run(cfg: Config) -> None:
    producer = _build_producer(cfg)
    logger.info(
        "Producer started. Polling %d feeds every 30s.", len(cfg.mta_feed_urls)
    )

    while True:
        for url in cfg.mta_feed_urls:
            fid = _feed_id(url)
            feed = _fetch_feed(url)
            if feed is None:
                continue

            tu_count = _parse_trip_updates(
                feed, fid, producer, cfg.kafka_topic_trip_updates
            )
            vp_count = _parse_vehicle_positions(
                feed, fid, producer, cfg.kafka_topic_vehicle_positions
            )
            al_count = _parse_alerts(
                feed, fid, producer, cfg.kafka_topic_alerts
            )
            producer.flush()
            logger.info(
                "feed=%s trip_updates=%d vehicle_positions=%d alerts=%d",
                fid, tu_count, vp_count, al_count,
            )

        time.sleep(30)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run(Config())
