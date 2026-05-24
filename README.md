# World Events Stocks

Kafka-based pipeline for publishing and consuming GDELT event messages.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- Python 3.14+

## Quick Start

Run all commands from the repository root.

1. Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Start Kafka infrastructure (ZooKeeper, Kafka, Schema Registry):

```bash
docker compose up -d zookeeper kafka schema-registry
```

3. (Optional) Start Kafka UIs:

```bash
docker compose up -d kafkaui kafdrop
```

4. Produce a sample GDELT event message:

```bash
python3 producers/gdelt/producer.py
```

5. Consume the message:

```bash
python3 consumers/gdelt/consumer.py
```

## Verify Services

Check service health:

```bash
docker compose -f docker/docker-compose.yml ps
```

Tail Kafka and Schema Registry logs:

```bash
docker compose -f docker/docker-compose.yml logs -f kafka schema-registry
```

## Shut Down

Stop and remove containers:

```bash
docker compose -f docker/docker-compose.yml down
```

