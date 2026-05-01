import json
import random
import time
import argparse
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC_RAW       = "traffic-raw"

JUNCTIONS = [
    {"sensor_id": "SENSOR-JA", "junction_name": "Galle Face Junction"},
    {"sensor_id": "SENSOR-JB", "junction_name": "Kollupitiya Junction"},
    {"sensor_id": "SENSOR-JC", "junction_name": "Borella Junction"},
    {"sensor_id": "SENSOR-JD", "junction_name": "Nugegoda Junction"},
]


CRITICAL_PROBABILITY = 0.05   # 5 %


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)



def generate_reading(junction: dict, critical: bool = False) -> dict:
    """Return a sensor reading dict for the given junction."""
    now = datetime.now(timezone.utc).isoformat()

    if critical:
        
        vehicle_count = random.randint(120, 200)
        avg_speed     = round(random.uniform(2.0, 9.9), 2)
    else:
        
        if random.random() < 0.70:
            vehicle_count = random.randint(10, 79)
            avg_speed     = round(random.uniform(25.0, 70.0), 2)
        else:
            vehicle_count = random.randint(80, 119)
            avg_speed     = round(random.uniform(11.0, 24.9), 2)

    return {
        "sensor_id":     junction["sensor_id"],
        "junction_name": junction["junction_name"],
        "timestamp":     now,
        "vehicle_count": vehicle_count,
        "avg_speed":     avg_speed,
    }



def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Connect to Kafka with retry logic (container startup may be slow)."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",          
                retries=3,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.warning(
                "Kafka not ready (attempt %d/%d). Retrying in %ds...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after %d attempts." % retries)


def delivery_callback(record_metadata):
    """Called on successful delivery."""
    
    log.debug(
        "Delivered → topic=%s partition=%d offset=%d",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )



def run(interval: float, duration: float):
    producer   = create_producer()
    start_time = time.time()
    msg_count  = 0
    alert_count = 0

    log.info(
        "Starting producer | interval=%.1fs | duration=%s | junctions=%d",
        interval,
        f"{duration}s" if duration else "∞",
        len(JUNCTIONS),
    )
    log.info("Publishing to Kafka topic: %s", TOPIC_RAW)
    log.info("─" * 60)

    try:
        while True:
            if duration and (time.time() - start_time) >= duration:
                log.info("Duration %ds reached. Stopping.", duration)
                break

            for junction in JUNCTIONS:
                is_critical = random.random() < CRITICAL_PROBABILITY
                reading     = generate_reading(junction, critical=is_critical)

                producer.send(
                    TOPIC_RAW,
                    value=reading,
                    key=junction["sensor_id"].encode("utf-8"),
                ).add_callback(delivery_callback)

                msg_count += 1

                if is_critical:
                    alert_count += 1
                    log.warning(
                        "⚠  CRITICAL EVENT | %-30s | vehicles=%3d | speed=%5.1f km/h",
                        reading["junction_name"],
                        reading["vehicle_count"],
                        reading["avg_speed"],
                    )
                else:
                    log.info(
                        "   Normal reading | %-30s | vehicles=%3d | speed=%5.1f km/h",
                        reading["junction_name"],
                        reading["vehicle_count"],
                        reading["avg_speed"],
                    )

            producer.flush()
            time.sleep(interval)

    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        elapsed = time.time() - start_time
        log.info("─" * 60)
        log.info(
            "Producer stopped | runtime=%.1fs | messages=%d | critical_events=%d",
            elapsed, msg_count, alert_count,
        )



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Smart City Traffic Kafka Producer")
    parser.add_argument(
        "--interval", type=float, default=1.0,
        help="Seconds between batches of sensor readings (default: 1)",
    )
    parser.add_argument(
        "--duration", type=float, default=0,
        help="Total run time in seconds; 0 = run forever (default: 0)",
    )
    args = parser.parse_args()
    run(interval=args.interval, duration=args.duration)