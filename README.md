# World Events Stocks

Kafka-based pipeline for publishing and consuming GDELT event messages.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- Python 3.14+

## Step 1: Getting Started

Run all commands from the repository root.

1. Start Kafka infrastructure (ZooKeeper, Kafka, Schema Registry):

```bash
docker compose up -d zookeeper kafka schema-registry
```

2. (Optional) Start Kafka UIs:

```bash
docker compose up -d kafkaui kafdrop
```

## Step 2: Ingest Event Stream

Start with a simple flow:

1. Verify your stack is running:

```bash
docker compose up -d zookeeper kafka schema-registry
docker compose ps
```

2. In terminal A, start consumer to watch messages:

```bash
python3 consumers/gdelt/consumer.py
```

3. In terminal B, publish latest GDELT records:

```bash
python3 producers/gdelt/producer.py --mode latest --max-records 200
```

5. For continuous streaming producer mode:

```bash
python3 producers/gdelt/producer.py --mode stream --max-records 200 --poll-interval-seconds 60
```

6. To run consumer in continuous mode with a fresh group:

```bash
python3 consumers/gdelt/consumer.py --group-id gdelt-consumer-stream --auto-offset-reset earliest
```

## Verify Services

Check service health:

```bash
docker compose ps
```

Tail Kafka and Schema Registry logs:

```bash
docker compose logs -f kafka schema-registry
```

## Shut Down

Stop and remove containers:

```bash
docker compose down
```

### Notes

- `--mode latest` downloads the newest GDELT GKG archive and publishes up to `--max-records` parsed events.
- `--mode stream` polls the latest GKG snapshot repeatedly and publishes only new event ids seen during the current run.
