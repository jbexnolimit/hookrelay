# Kafka Setup

This directory documents the local Apache Kafka setup used by HookRelay for asynchronous webhook delivery in Ubuntu on WSL 2.

## Requirements

- Ubuntu 24.04 LTS on WSL 2
- Java 21
- Apache Kafka 4.3.1
- HookRelay Django backend and Rust worker

## Installation

Download Apache Kafka from the [official Kafka downloads page](https://kafka.apache.org/downloads/).

For this development environment, Kafka is installed outside the repository at:

```text
~/apps/kafka_2.13-4.3.1
```

Download and extract the distribution:

```bash
mkdir -p ~/apps
cd ~/apps

wget https://archive.apache.org/dist/kafka/4.3.1/kafka_2.13-4.3.1.tgz
tar -xzf kafka_2.13-4.3.1.tgz
```

Verify Java:

```bash
java -version
```

The Kafka distribution, local storage, and runtime files are not included in this repository.

## Configuration

HookRelay uses Kafka in KRaft mode. ZooKeeper is not required.

The default local broker address is:

```text
127.0.0.1:9092
```

Kafka uses the configuration file:

```text
~/apps/kafka_2.13-4.3.1/config/server.properties
```

### Initialize Kafka storage

Initialize storage only when setting up a fresh Kafka data directory. Do not repeat this operation on an existing initialized directory, because doing so may prevent access to existing local Kafka data.

```bash
cd ~/apps/kafka_2.13-4.3.1

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c config/server.properties
```

The cluster ID is generated for the new Kafka environment. Keep the existing initialized storage and cluster configuration when restarting the same broker.

## Start Kafka

Open an Ubuntu terminal and run:

```bash
cd ~/apps/kafka_2.13-4.3.1

bin/kafka-server-start.sh config/server.properties
```

Keep this terminal open while using HookRelay.

## Create the Delivery Topic

Open a second Ubuntu terminal:

```bash
cd ~/apps/kafka_2.13-4.3.1

bin/kafka-topics.sh \
  --create \
  --topic hookrelay.delivery-tasks \
  --bootstrap-server 127.0.0.1:9092 \
  --partitions 1 \
  --replication-factor 1
```

If the topic already exists, do not recreate it. Verify the existing topic instead:

```bash
bin/kafka-topics.sh \
  --list \
  --bootstrap-server 127.0.0.1:9092
```

Inspect the topic configuration:

```bash
bin/kafka-topics.sh \
  --describe \
  --topic hookrelay.delivery-tasks \
  --bootstrap-server 127.0.0.1:9092
```

## HookRelay Configuration

Configure the Django backend in `backend/.env` using the variable names expected by the current backend code:

```dotenv
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_DELIVERY_TOPIC=hookrelay.delivery-tasks
```

Configure the Rust worker in `rust-worker/.env`:

```dotenv
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_TOPIC=hookrelay.delivery-tasks
KAFKA_GROUP_ID=hookrelay-rust-worker
DJANGO_API_BASE_URL=http://127.0.0.1:8001
HTTP_TIMEOUT_SECONDS=10
```

Keep any other required environment variables in the respective files. Do not commit `.env` files or expose local credentials.

The variable names above should match the application configuration. If the source code changes, update this documentation accordingly.

## Verify the Setup

1. Start PostgreSQL.
2. Start the Kafka broker.
3. Start the Django development server on port `8001`.
4. Start the Rust worker.
5. Submit an event through the HookRelay API.
6. Confirm that the worker receives the task, calls the webhook receiver, reports the result to Django, and commits the Kafka message.

For example, submit a test event:

```bash
curl -i -X POST http://127.0.0.1:8001/api/events/ \
  -H 'Content-Type: application/json' \
  -d '{
    "event_type": "order.created",
    "payload": {
      "order_id": "ORD-2026-1022",
      "total": 1499,
      "currency": "PHP"
    }
  }'
```

Check the Rust worker logs for the received task, HTTP response, result reporting, and Kafka commit.

### Consume Kafka Messages for Debugging

To inspect messages from the beginning of the topic, run:

```bash
cd ~/apps/kafka_2.13-4.3.1

bin/kafka-console-consumer.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --topic hookrelay.delivery-tasks \
  --from-beginning
```

This command reads existing messages and continues waiting for new ones. Use it for development and debugging; do not treat it as a substitute for checking the worker and database results.

Note that consuming messages with a separate consumer group does not demonstrate that the Rust worker processed them successfully.

## Troubleshooting

### Kafka is unavailable

Check whether the broker is running and the topic exists:

```bash
cd ~/apps/kafka_2.13-4.3.1

bin/kafka-topics.sh \
  --list \
  --bootstrap-server 127.0.0.1:9092
```

### The worker cannot connect

Check that:

- Kafka is running on `127.0.0.1:9092`.
- The topic is named `hookrelay.delivery-tasks`.
- The Rust worker has the correct environment variables.
- The Django API is available at `http://127.0.0.1:8001`.

### Resetting local Kafka data

Do not delete Kafka storage or reformat the cluster just to clear test messages. If you need a fresh topic for testing, inspect the existing topic and consumer state first, then choose an appropriate cleanup procedure.

## Notes

- This configuration is intended for local development and demonstration.
- One partition and a replication factor of one are appropriate for this single-broker development setup, not production high availability.
- Keep Kafka storage, logs, and runtime data outside the Git repository.
- Production deployments require appropriate security, replication, monitoring, error handling, and operational configuration.

## References

- [Apache Kafka Downloads](https://kafka.apache.org/downloads/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)