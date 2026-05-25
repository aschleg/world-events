from confluent_kafka import DeserializingConsumer
from confluent_kafka.error import ConsumeError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

with open("producers/gdelt/schemas/gdelt_event.avsc", "r") as f:
    schema_str = f.read()

avro_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client,
    schema_str=schema_str,
    from_dict=lambda obj, ctx: obj
)

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer,
    'group.id': 'gdelt-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['gdelt-events'])

try:
    msg = consumer.poll(10.0)
    if msg is None:
        print("No message received")
    elif msg.error():
        print(f"Error: {msg.error()}")
    else:
        print("Received message:", msg.value())
except ConsumeError as exc:
    if "UNKNOWN_TOPIC_OR_PART" in str(exc):
        print("Topic 'gdelt-events' does not exist yet.")
        print("Run: python3 producers/gdelt/producer.py --mode sample")
    else:
        print(f"Consume error: {exc}")
finally:
    consumer.close()
