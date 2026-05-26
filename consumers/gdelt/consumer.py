import argparse
import time

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
    from_dict=lambda obj, 
    ctx: obj
)

def parse_args():
    parser = argparse.ArgumentParser(description="Consume GDELT events from Kafka")
    parser.add_argument(
        "--group-id",
        default="gdelt-consumer-group",
        help="Kafka consumer group id",
    )
    parser.add_argument(
        "--auto-offset-reset",
        choices=["earliest", "latest"],
        default="earliest",
        help="Offset behavior when group has no committed offsets",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after consuming this many messages (0 means run forever)",
    )

    return parser.parse_args()


def create_consumer(group_id, auto_offset_reset):
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
        'group.id': group_id,
        'auto.offset.reset': auto_offset_reset,
    }
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(['gdelt-events'])

    return consumer


def main():
    args = parse_args()
    consumer = create_consumer(args.group_id, args.auto_offset_reset)
    consumed_count = 0

    print(
        "Starting consumer "
        f"(group_id={args.group_id}, auto_offset_reset={args.auto_offset_reset}, "
        f"max_messages={args.max_messages})"
    )

    try:
        while True:
            try:
                msg = consumer.poll(1)
            except ConsumeError as exc:
                if "UNKNOWN_TOPIC_OR_PART" in str(exc):
                    print("Topic 'gdelt-events' does not exist yet. Waiting for producer...")
                    time.sleep(2)
                    continue
                print(f"Consume error: {exc}")
                continue

            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            consumed_count += 1
            print(f"Received message #{consumed_count}: {msg.value()}")

            if args.max_messages > 0 and consumed_count >= args.max_messages:
                print(f"Reached max_messages={args.max_messages}, stopping consumer")
                break
    except KeyboardInterrupt:
        print("Stopping consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
