
# HookRelay

A webhook delivery system built with Django REST Framework, PostgreSQL, Apache Kafka, and a Rust worker.

HookRelay accepts events through a REST API, creates delivery tasks for matching webhook endpoints, processes those tasks asynchronously through Kafka, sends HTTP requests to receivers, and reports delivery results back to Django.

## Tech Stack

- **Backend:** Python, Django, Django REST Framework
- **Database:** PostgreSQL
- **Message Broker:** Apache Kafka (KRaft mode)
- **Worker:** Rust
- **HTTP:** REST APIs and webhook receivers
- **Development Environment:** Ubuntu 24.04 LTS on WSL 2

## Project Structure

```text
hookrelay/
├── backend/
│   ├── apps/
│   │   ├── analytics/
│   │   ├── core/
│   │   ├── deliveries/
│   │   ├── devtools/
│   │   ├── events/
│   │   └── webhooks/
│   ├── config/
│   ├── manage.py
│   └── requirements.txt
├── rust-worker/
│   ├── src/
│   ├── Cargo.toml
│   └── Cargo.lock
├── infra/
│   └── kafka/
│       └── README.md
├── docker-compose.yml
└── README.md
```

## Prerequisites

The instructions below assume Ubuntu 24.04 running on WSL 2.

Install the required packages:

```bash
sudo apt update
sudo apt install -y \
  git curl wget \
  build-essential cmake clang pkg-config \
  python3 python3-pip python3-venv python3-dev \
  libssl-dev zlib1g-dev \
  libcurl4-openssl-dev libsasl2-dev \
  postgresql postgresql-contrib postgresql-client \
  openjdk-21-jdk
```

Install Rust using the official installer:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
```

Verify the installations:

```bash
python3 --version
cargo --version
rustc --version
java -version
psql --version
```

## 1. Set Up the Project

Clone the repository and enter the project directory:

```bash
git clone https://github.com/jbexnolimit/hookrelay.git
cd hookrelay
```

Configure Git to normalize Windows line endings when adding files:

```bash
git config --local core.autocrlf input
```

Create and activate the Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the backend dependencies:

```bash
cd backend
pip install -r requirements.txt
```

Return to the repository root when needed:

```bash
cd ..
```

## 2. Configure PostgreSQL

Start PostgreSQL:

```bash
sudo service postgresql start
```

Open the PostgreSQL administrative shell:

```bash
sudo -u postgres psql
```

Create a development role and database:

```sql
CREATE ROLE hookrelay_user WITH LOGIN CREATEDB;
CREATE DATABASE hookrelay_db OWNER hookrelay_user;
\password hookrelay_user
\q
```

If the role or database already exists, do not recreate it. Reuse your existing development setup.

Test the connection:

```bash
psql -h 127.0.0.1 -U hookrelay_user -d hookrelay_db -W
```

Inside `psql`, run:

```sql
SELECT current_database(), current_user;
\q
```

### Backend environment variables

Create or update `backend/.env` using the variables expected by `backend/config/settings.py`.

At minimum, configure the database connection:

```dotenv
DB_NAME=hookrelay_db
DB_USER=hookrelay_user
DB_PASSWORD=your_local_database_password
DB_HOST=127.0.0.1
DB_PORT=5432
```

Keep the project's other required environment variables. Use your own local credentials, and never commit `.env` files.

Run the migrations and seed the demo receivers:

```bash
cd ~/projects/hookrelay/backend
source ~/projects/hookrelay/.venv/bin/activate

python manage.py check
python manage.py migrate
python manage.py seed_demo
```

The demo seed command creates the Inventory Demo Receiver and Failure Demo Receiver.

## 3. Set Up Apache Kafka

Download and extract Kafka outside the repository:

```bash
mkdir -p ~/apps
cd ~/apps

wget https://archive.apache.org/dist/kafka/4.3.1/kafka_2.13-4.3.1.tgz
tar -xzf kafka_2.13-4.3.1.tgz

cd ~/apps/kafka_2.13-4.3.1
```

Kafka runs in KRaft mode and does not require ZooKeeper.

### Initialize Kafka storage

**Only perform this step when initializing a fresh Kafka data directory.** Do not reformat an existing initialized Kafka storage directory.

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format \
  --standalone \
  -t "$KAFKA_CLUSTER_ID" \
  -c config/server.properties
```

### Start the Kafka broker

Keep the broker running in its own terminal:

```bash
cd ~/apps/kafka_2.13-4.3.1
bin/kafka-server-start.sh config/server.properties
```

### Create the delivery topic

In a separate terminal:

```bash
cd ~/apps/kafka_2.13-4.3.1

bin/kafka-topics.sh \
  --create \
  --topic hookrelay.delivery-tasks \
  --bootstrap-server 127.0.0.1:9092 \
  --partitions 1 \
  --replication-factor 1
```

If the topic already exists, keep it and verify it:

```bash
bin/kafka-topics.sh \
  --list \
  --bootstrap-server 127.0.0.1:9092
```

The expected topic is:

```text
hookrelay.delivery-tasks
```

See [infra/kafka/README.md](infra/kafka/README.md) for additional Kafka notes.

## 4. Configure and Run the Rust Worker

The Rust worker uses environment variables from `rust-worker/.env`.

Ensure the file contains the required settings, keeping any existing required variables:

```dotenv
KAFKA_BROKERS=127.0.0.1:9092
KAFKA_TOPIC=hookrelay.delivery-tasks
KAFKA_GROUP_ID=hookrelay-rust-worker
DJANGO_API_BASE_URL=http://127.0.0.1:8001
HTTP_TIMEOUT_SECONDS=10
```

Do not commit credentials or private configuration.

Build the worker:

```bash
cd ~/projects/hookrelay/rust-worker
cargo check
cargo test
```

Run the worker:

```bash
cargo run
```

Keep the worker running in its own terminal.

## 5. Start the Django API

Open another terminal:

```bash
cd ~/projects/hookrelay/backend
source ~/projects/hookrelay/.venv/bin/activate

python manage.py runserver 8001
```

The API is available at:

```text
http://127.0.0.1:8001/
```

Useful endpoints:

| Endpoint | Purpose |
|---|---|
| `/api/webhooks/` | Manage webhook endpoints |
| `/api/events/` | Create and retrieve events |
| `/api/deliveries/` | Inspect deliveries |
| `/api/analytics/` | View delivery analytics |
| `/api/dev/receiver/success/` | Demo receiver that succeeds |
| `/api/dev/receiver/fail/` | Demo receiver that fails |

## 6. Run an End-to-End Test

Ensure PostgreSQL, Kafka, Django, and the Rust worker are running.

Submit a test event from another terminal:

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

The expected workflow is:

1. Django creates the event and its matching delivery.
2. Django publishes the delivery task to Kafka.
3. The Rust worker consumes the task.
4. The worker sends an HTTP request to the webhook receiver.
5. The worker reports the result to Django.
6. Django records the delivery result in PostgreSQL.
7. The worker commits the Kafka message after processing.

For a successful receiver, expect HTTP 200 from the receiver and a successful delivery result recorded by Django.

Check the Django and worker logs if any step fails.

## 7. Troubleshooting

### Kafka connection errors

Check that the broker is running and the topic exists:

```bash
cd ~/apps/kafka_2.13-4.3.1
bin/kafka-topics.sh --list --bootstrap-server 127.0.0.1:9092
```

### PostgreSQL connection errors

Check the PostgreSQL cluster:

```bash
pg_lsclusters
sudo service postgresql status
```

Verify the database name, username, password, host, and port in `backend/.env`.

### Rust build errors

Confirm that native build dependencies are installed:

```bash
sudo apt install -y \
  build-essential cmake clang pkg-config \
  libssl-dev zlib1g-dev \
  libcurl4-openssl-dev libsasl2-dev
```

Then retry:

```bash
cd ~/projects/hookrelay/rust-worker
cargo check
```

### Django errors

Activate the correct virtual environment and run:

```bash
cd ~/projects/hookrelay/backend
source ~/projects/hookrelay/.venv/bin/activate

python manage.py check
python manage.py showmigrations
```

## Development Notes

- Run the project from the Linux filesystem under `~/projects/hookrelay`.
- Keep the Kafka installation and runtime data outside the repository.
- Keep `.env`, local credentials, Python virtual environments, and Rust build artifacts out of Git.
- The included development server and single-broker Kafka configuration are for local development, not production deployment.
- `cargo test` passing with zero discovered tests does not constitute comprehensive worker test coverage.
