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

## Step 2: Ingest Event Stream (Where To Start)

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

4. For quick smoke test, publish one sample event:

```bash
python3 producers/gdelt/producer.py --mode sample
```

### Notes

- `--mode latest` downloads the newest GDELT GKG archive and publishes up to `--max-records` parsed events.
- Current consumer reads one message and exits. Re-run it to inspect additional records.
- For real-time learning, next improvement is a long-running consumer loop and a scheduled/continuous producer.

