import argparse
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from producers.gdelt.fetch_latest_gkg import fetch_and_parse_gkg


SCHEMA_PATH = "producers/gdelt/schemas/gdelt_event.avsc"
TOPIC = "gdelt-events"

with open(SCHEMA_PATH, "r") as schema_file:
    schema_str = schema_file.read()

schema_registry_conf = {
    "url": "http://localhost:8081"
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=schema_str,
    to_dict=lambda obj, ctx: obj
)

producer_conf = {
    "bootstrap.servers": "localhost:9092",
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer
}
producer = SerializingProducer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] [{msg.offset()}]")


def publish_event(event):
    producer.produce(
        topic=TOPIC,
        key=event["event_id"],
        value=event,
        on_delivery=delivery_report,
    )


def publish_latest(max_records):
    events = fetch_and_parse_gkg(max_records=max_records)
    print(f"Publishing {len(events)} events to topic '{TOPIC}'")

    for event in events:
        publish_event(event)

    producer.flush()


def stream_latest(max_records, poll_interval_seconds):
    seen_event_ids = set()
    print(
        f"Starting stream mode for topic '{TOPIC}' "
        f"(max_records={max_records}, poll_interval_seconds={poll_interval_seconds})"
    )

    while True:
        events = fetch_and_parse_gkg(max_records=max_records)
        new_events = [event for event in events if event["event_id"] not in seen_event_ids]

        if new_events:
            print(f"Publishing {len(new_events)} new events")
            for event in new_events:
                publish_event(event)
                seen_event_ids.add(event["event_id"])

            producer.flush()
        else:
            print("No new events found in latest GKG snapshot")

        time.sleep(poll_interval_seconds)


def parse_args():
    parser = argparse.ArgumentParser(description="Produce GDELT events to Kafka")
    parser.add_argument(
        "--mode",
        choices=["latest", "stream"],
        default="latest",
        help="latest runs one batch; stream runs continuously",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=200,
        help="Maximum number of latest GKG rows to publish when mode=latest",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=60,
        help="Polling interval for mode=stream",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        if args.mode == "latest":
            publish_latest(max_records=args.max_records)
        else:
            stream_latest(
                max_records=args.max_records,
                poll_interval_seconds=args.poll_interval_seconds,
            )
    except KeyboardInterrupt:
        print("Stopping producer stream")
        producer.flush()


if __name__ == "__main__":
    main()
