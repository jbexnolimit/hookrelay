# HookRelay

HookRelay is a webhook delivery system built with Django, PostgreSQL, Apache Kafka, and Rust. It receives events, creates delivery tasks for subscribed webhook endpoints, and processes deliveries asynchronously using a Rust worker.

## Features

- Event ingestion through REST APIs
- Webhook endpoint registration and event subscriptions
- Asynchronous delivery processing using Apache Kafka
- Rust-based worker for HTTP webhook delivery
- Delivery status and attempt tracking
- Automatic retry scheduling with configurable backoff
- Recovery of stale or interrupted deliveries
- Idempotent delivery callback handling
- Delivery analytics and monitoring
- Docker Compose infrastructure for PostgreSQL and Kafka

## Tech Stack

- **Backend:** Python, Django, Django REST Framework
- **Database:** PostgreSQL
- **Messaging:** Apache Kafka (KRaft)
- **Worker:** Rust
- **Infrastructure:** Docker, Docker Compose
- **Development Environment:** WSL2, Ubuntu

## Project Structure

```text
hookrelay/
├── backend/
│   ├── apps/
│   │   ├── analytics/
│   │   ├── deliveries/
│   │   ├── events/
│   │   ├── webhooks/
│   │   └── devtools/
│   ├── config/
│   ├── manage.py
│   └── .env
│
├── rust-worker/
│   ├── src/
│   ├── Cargo.toml
│   └── .env
│
├── docker-compose.yml
├── .env
└── README.md
```

## Requirements

- Ubuntu on WSL2 or a Linux environment
- Python 3.12+
- Rust and Cargo
- Docker Desktop with WSL2 integration, or Docker Engine with Docker Compose
- Git

PostgreSQL and Kafka run through Docker Compose, so you do not need to install them separately.

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/jbexnolimit/hookrelay.git
cd hookrelay
```

### 2. Configure environment variables

Create the root `.env` file:

```env
DB_NAME=hookrelay_db
DB_USER=hookrelay_user
DB_PASSWORD=your_database_password
DB_PORT=5433
```

Create `backend/.env`:

```env
DB_NAME=hookrelay_db
DB_USER=hookrelay_user
DB_PASSWORD=your_database_password
DB_HOST=127.0.0.1
DB_PORT=5433

KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
KAFKA_DELIVERY_TOPIC=hookrelay.delivery-tasks
```

Create `rust-worker/.env`:

```env
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_TOPIC=hookrelay.delivery-tasks
KAFKA_GROUP_ID=hookrelay-rust-worker

DJANGO_API_BASE_URL=http://127.0.0.1:8001
HTTP_TIMEOUT_SECONDS=10
```

Use the same database credentials in the root and Django environment files.

### 3. Start PostgreSQL and Kafka

Start the infrastructure:

```bash
docker compose up -d
```

Docker Compose starts:

- PostgreSQL 16
- Apache Kafka 4.3.1 in KRaft mode
- Kafka topic initialization

The Kafka initialization service automatically creates the `hookrelay.delivery-tasks` topic if it does not already exist.

Check service status:

```bash
docker compose ps
```

Verify the Kafka topic:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Verify PostgreSQL:

```bash
docker compose exec postgres \
  pg_isready -U hookrelay_user -d hookrelay_db
```

Both PostgreSQL and Kafka include health checks.

### 4. Set up the Django backend

Create and activate a virtual environment if you do not already have one:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r backend/requirements.txt
```

Run database migrations:

```bash
cd backend
python manage.py migrate
```

Start the Django API:

```bash
python manage.py runserver 0.0.0.0:8001
```

The API is available at:

```text
http://127.0.0.1:8001
```

### 5. Start the Rust worker

Open another terminal:

```bash
cd ~/projects/hookrelay/rust-worker
cargo run
```

The worker connects to Kafka, consumes delivery tasks, sends HTTP requests to webhook endpoints, and reports delivery results back to Django.

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET, POST | `/api/webhooks/` | Manage webhook endpoints |
| GET, POST | `/api/events/` | Submit and retrieve events |
| GET | `/api/deliveries/` | View delivery records |
| GET | `/api/analytics/` | View delivery analytics |
| POST | `/api/dev/receiver/success/` | Successful demo receiver |
| POST | `/api/dev/receiver/fail/` | Failing demo receiver |
| POST | `/api/dev/receiver/recover/` | Receiver that fails temporarily, then succeeds |

### Demo receivers

Create or update the demo webhook endpoints after applying database migrations:

```bash
cd backend
python manage.py seed_demo
```

This command configures three demo endpoints:

- **Inventory Demo Receiver:** returns HTTP 200 for `order.created` events.
- **Failure Demo Receiver:** returns HTTP 500 for `payment.failed` events.
- **Recovery Demo Receiver:** returns HTTP 500 for its first two requests by default, then HTTP 200.

The recovery receiver's failure threshold is configurable through Django admin. The demo receiver routes are intended for development and are only included when Django `DEBUG` is enabled.

### Example: Submit an event

```bash
curl -X POST http://127.0.0.1:8001/api/events/ \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "order.created",
    "payload": {
      "order_ref": "ORD-1001",
      "amount_minor": 200000,
      "currency": "PHP",
      "status": "placed"
    }
  }'
```

The API stores the event, finds active webhook subscriptions matching its event type, creates delivery records, and records delivery tasks in the PostgreSQL transactional outbox. The outbox dispatcher publishes those tasks to Kafka.

### View deliveries

```bash
curl http://127.0.0.1:8001/api/deliveries/
```

Delivery records include:

- Delivery status
- Attempt count and maximum attempts
- Last HTTP response status
- Last attempt timestamp
- Next retry time
- Delivery error details

## How Delivery Processing Works

1. A client submits an event to the Django API.
2. Django stores the event in PostgreSQL.
3. Matching active webhook endpoints are identified based on their subscribed event types.
4. Django creates delivery records and records pending delivery tasks in the transactional outbox within the same database transaction.
5. The outbox dispatcher publishes pending tasks to Kafka after the database transaction commits.
6. The Rust worker consumes the tasks.
7. The worker sends HTTP POST requests to the target webhook endpoints.
8. The worker reports the delivery result to Django.
9. Django updates the delivery status and records the attempt.
10. The worker commits the Kafka message after processing.

### Outgoing delivery idempotency

Each outgoing webhook request includes an `Idempotency-Key` HTTP header
with the format `hookrelay-delivery-{delivery_id}`. The key identifies the
logical delivery, so retries and Kafka replays of that delivery reuse the
same key.

Webhook receivers that need duplicate protection should persist and enforce
this key atomically with the business operation it protects. Repeated
requests with the same key should not repeat an already-applied operation.
The receiver should define how it handles repeated keys with different
request payloads and how long it retains idempotency records.

The key alone does not guarantee exactly-once processing. HookRelay uses
at-least-once delivery behavior, so a receiver may receive a request again
if the worker or network fails before the result is recorded. Each reported
delivery attempt has its own `attempt_id`; this is distinct from the stable
idempotency key for the logical delivery.

### Claim-token protection

Each published delivery task carries a unique claim token. The Rust worker
returns that token with its result callback, and Django accepts the callback
only when the token matches the delivery's current claim and the delivery is
still processing. Callbacks from obsolete claims are acknowledged but ignored,
so an old worker cannot overwrite a newer delivery state.

Publishing-failure handlers also check the claim token before reverting a
delivery. Stale-delivery recovery clears the old token before the delivery is
claimed again. This protects database state from stale workers, but it cannot
cancel an HTTP request that is already in flight; receiver-side idempotency
remains important.

## Transactional Outbox

HookRelay stores delivery-task publication intent in PostgreSQL before attempting
to publish a task to Kafka. Event creation, delivery claim updates, and outbox
creation occur in the same database transaction. Manual retries and scheduled
retries use the same mechanism.

The `dispatch_outbox` management command publishes pending outbox entries and
waits for Kafka acknowledgement. Successful entries are marked as published.
Failed publications remain pending and are rescheduled with exponential
backoff. A dispatch lease allows abandoned entries to become eligible for
another attempt if a dispatcher process stops unexpectedly.

The outbox closes the gap where a database transaction could commit but its
Kafka publication intent could be lost. It does not provide exactly-once
delivery: a process may publish a message and stop before recording the
acknowledgement in PostgreSQL, causing the message to be published again.
Claim tokens protect delivery state from obsolete callbacks, and receiver-side
idempotency is still recommended.

## Retry Handling

HookRelay automatically retries eligible failed deliveries.

**Retryable failures:**
- Network and connection failures
- HTTP 408 and 429
- HTTP 5xx responses

Other HTTP 4xx responses are treated as permanent failures.

**Retry schedule:**

| Retry | Delay |
|---|---:|
| 1 | 30 seconds |
| 2 | 60 seconds |
| 3 | 120 seconds |
| 4 | 240 seconds |

The system allows up to 5 total delivery attempts.

### Retry scheduler

The `process_delivery_retries` command identifies deliveries due for retry, records new delivery claims in the outbox, and recovers stale deliveries interrupted while queued or processing. The separate `dispatch_outbox` command publishes pending outbox entries to Kafka.

Run manually:

```bash
cd backend

python manage.py process_delivery_retries \
  --batch-size 100 \
  --stale-after-seconds 300
```

For continuous retry processing and outbox publication during development, run the combined scheduler in a separate terminal:

```bash
cd ~/projects/hookrelay/backend
python manage.py run_delivery_retry_scheduler --interval 10
```

Each cycle runs `process_delivery_retries` and `dispatch_outbox` independently. If one command fails, the other is still attempted. The scheduler checks every 10 seconds by default. Press `Ctrl+C` to stop it. Adjust the interval with `--interval`, the batch size with `--batch-size`, and the stale-delivery timeout with `--stale-after-seconds`.

You can also run either command manually:

```bash
python manage.py dispatch_outbox --batch-size 100
python manage.py process_delivery_retries --batch-size 100 --stale-after-seconds 300
```

For a cron-based alternative, run both commands periodically. Use a semicolon between them so outbox dispatch is still attempted if retry processing fails.

Create its log directory:

```bash
mkdir -p ~/.local/state/hookrelay
```

Add this to your crontab using `crontab -e`:

```cron
* * * * * /usr/bin/flock -n /tmp/hookrelay-outbox-scheduler.lock /bin/bash -lc 'cd "$HOME/projects/hookrelay/backend"; "$HOME/projects/hookrelay/.venv/bin/python" manage.py process_delivery_retries --batch-size 100 --stale-after-seconds 300; "$HOME/projects/hookrelay/.venv/bin/python" manage.py dispatch_outbox --batch-size 100' >> "$HOME/.local/state/hookrelay/retry-scheduler.log" 2>&1
```

View scheduler logs:

```bash
tail -f ~/.local/state/hookrelay/retry-scheduler.log
```

The WSL environment must be running for its cron service and local worker processes to operate.

## Running Tests

Run the delivery application tests:

```bash
cd backend
python manage.py test apps.deliveries
```

The delivery test suite covers:

- Successful and failed delivery handling
- Retry eligibility and scheduling
- Permanent failure handling
- Stale delivery recovery
- Kafka publication failures and outbox retries
- Transactional outbox creation and dispatch
- Callback idempotency
- Late delivery callbacks

## Dead-Letter Queue (DLQ)

The Rust worker sends malformed delivery-task messages and empty Kafka
messages to `hookrelay.delivery-tasks.dlq` by default. Set `KAFKA_DLQ_TOPIC`
to override the destination.

Each DLQ record is a JSON envelope containing the source topic, partition,
offset, rejection reason, rejection timestamp, and original payload bytes.
The original payload is represented as an array of integers, or `null` if
the Kafka message has no payload.

The worker commits the source offset only after DLQ publication succeeds.
If publication fails, it exits without committing the rejected message.
Restart it with a working DLQ destination and the same consumer group to
retry the message. Delivery is at-least-once: duplicate DLQ records are
possible if publishing succeeds but committing the source offset fails.

### Inspect the DLQ

These commands assume Kafka is running in the `hookrelay-kafka` container.

Describe the topic:

```bash
docker exec hookrelay-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hookrelay.delivery-tasks.dlq
```

Read records from the beginning; press `Ctrl+C` to stop:

```bash
docker exec hookrelay-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hookrelay.delivery-tasks.dlq --from-beginning
```

### Safe recovery

Inspect the rejection reason and source location. Decode and validate the
original payload, fix the underlying issue, and only then consider replay.
Check for duplicate side effects before replaying. HookRelay does not
currently provide an automatic replay command; avoid blind replay or
consumer offset resets.

### Integration test

Run `./scripts/test-dlq-integration.sh` from the repository root. It requires
Docker, the running Kafka container, Rust/Cargo, and Python 3. It verifies
that failed DLQ publication does not advance the source offset and that
recovery publishes the envelope and commits the offset. Unique test topics
are retained for inspection and are not deleted automatically.

## Docker Commands

Start infrastructure:

```bash
docker compose up -d
```

View running containers:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f postgres kafka
```

Stop infrastructure:

```bash
docker compose down
```

Docker Compose uses named volumes to persist PostgreSQL and Kafka data. Stopping the containers does not remove this data.

**Warning:** Avoid `docker compose down -v` unless you intentionally want to delete the persistent volumes and their data.

## Troubleshooting

### Messages are not reaching the DLQ

- Check the worker's `KAFKA_DLQ_TOPIC` and Kafka connectivity.
- Look for `DLQ publication failed; source message will not be committed` in worker logs.
- Correct the destination or permissions, then restart with the same consumer group to retry the uncommitted message.


**Docker permission denied**

Check Docker access:

```bash
docker run --rm hello-world
```

If necessary, add your WSL user to the Docker group and reopen the terminal:

```bash
sudo usermod -aG docker "$USER"
```

**PostgreSQL connection refused**

Check the container:

```bash
docker compose ps
docker compose logs postgres
```

Confirm that Django uses `127.0.0.1:5433`.

**Kafka connection refused**

Check Kafka health:

```bash
docker compose ps
docker compose logs kafka
```

Verify the topic:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

**Worker is not processing deliveries**

Check that Kafka is healthy, the topic exists, and the Rust worker environment points to `127.0.0.1:9092`.

Also confirm that the Django API is running and accessible at `http://127.0.0.1:8001`.

## Development Notes

- Docker Compose manages PostgreSQL and Kafka; Django and the Rust worker run locally during development.
- PostgreSQL and Kafka data are persisted using Docker named volumes.
- The retry scheduler runs separately from the Django API and Rust worker; it also dispatches pending outbox entries.
- Local development configuration is intended for testing and development, not production deployment.
