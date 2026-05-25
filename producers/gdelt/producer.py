import argparse
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

try:
    from producers.gdelt.fetch_latest_gkg import fetch_and_parse_gkg
except ModuleNotFoundError:
    from fetch_latest_gkg import fetch_and_parse_gkg


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

sample_event = {
    "event_id": "20250630121500",
    "timestamp": "2025-06-30T12:15:00Z",
    "themes": ["ECON_STOCKMARKET", "GEOPOLITICS"],
    "organizations": ["Tesla", "OpenAI"],
    "locations": ["United States", "California"],
    "tone": -2.3
}

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


def publish_sample():
    publish_event(sample_event)
    producer.flush()


def publish_latest(max_records):
    events = fetch_and_parse_gkg(max_records=max_records)
    print(f"Publishing {len(events)} events to topic '{TOPIC}'")

    for event in events:
        publish_event(event)

    producer.flush()


def parse_args():
    parser = argparse.ArgumentParser(description="Produce GDELT events to Kafka")
    parser.add_argument(
        "--mode",
        choices=["sample", "latest"],
        default="sample",
        help="sample sends a single hardcoded event; latest downloads latest GKG file",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=200,
        help="Maximum number of latest GKG rows to publish when mode=latest",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.mode == "sample":
        publish_sample()
    else:
        publish_latest(max_records=args.max_records)


if __name__ == "__main__":
    main()
