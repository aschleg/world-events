import json
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


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
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] [{msg.offset()}] {msg.value()}")

producer.produce(topic=TOPIC, key=sample_event["event_id"], value=sample_event, on_delivery=delivery_report)
producer.flush()
