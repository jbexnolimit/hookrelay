#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOOTSTRAP="${KAFKA_TEST_BROKERS:-127.0.0.1:9092}"
KAFKA_CONTAINER="${KAFKA_TEST_CONTAINER:-hookrelay-kafka}"
SUFFIX="$(date +%s)-$$"

SOURCE_TOPIC="hookrelay.dlq-it-source-${SUFFIX}"
DLQ_TOPIC="hookrelay.dlq-it-dlq-${SUFFIX}"
GROUP_ID="hookrelay-dlq-it-${SUFFIX}"
INVALID_DLQ="invalid topic name"

KAFKA_BIN="/opt/kafka/bin"
WORKER_LOG="$(mktemp)"
WORKER_PID=""

cleanup() {
    if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
        kill "${WORKER_PID}" 2>/dev/null || true
        wait "${WORKER_PID}" 2>/dev/null || true
    fi
    rm -f "${WORKER_LOG}"
}
trap cleanup EXIT

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    if [[ -s "${WORKER_LOG}" ]]; then
        printf '\n========== WORKER LOG ==========\n' >&2
        cat "${WORKER_LOG}" >&2
    fi
    exit 1
}

kafka_cli() {
    docker exec "${KAFKA_CONTAINER}" "$@"
}

create_topic() {
    kafka_cli "${KAFKA_BIN}/kafka-topics.sh" \
        --bootstrap-server localhost:9092 \
        --create --if-not-exists \
        --topic "$1" --partitions 1 --replication-factor 1 >/dev/null
}

wait_for_topic() {
    local topic="$1"
    local attempt

    for attempt in $(seq 1 20); do
        if kafka_cli "${KAFKA_BIN}/kafka-topics.sh" \
            --bootstrap-server localhost:9092 \
            --describe --topic "${topic}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done

    fail "Kafka topic did not become available: ${topic}"
}

publish_raw_message() {
    local topic="$1"
    local payload="$2"

    printf '%s\n' "${payload}" |
        docker exec -i "${KAFKA_CONTAINER}" \
            "${KAFKA_BIN}/kafka-console-producer.sh" \
            --bootstrap-server localhost:9092 \
            --topic "${topic}" >/dev/null
}

group_description() {
    kafka_cli "${KAFKA_BIN}/kafka-consumer-groups.sh" \
        --bootstrap-server localhost:9092 \
        --describe --group "${GROUP_ID}" 2>&1 || true
}

wait_for_committed_offset() {
    local attempt
    local output

    for attempt in $(seq 1 30); do
        output="$(group_description)"
        if awk -v topic="${SOURCE_TOPIC}" \
            '$2 == topic && $4 == "1" { found = 1 }
             END { exit !found }' <<<"${output}"; then
            return 0
        fi
        sleep 1
    done

    printf '%s\n' "${output}" >&2
    fail "Source offset was not committed as expected"
}

wait_for_worker_log() {
    local pattern="$1"
    local attempt

    for attempt in $(seq 1 30); do
        if grep -Fq "${pattern}" "${WORKER_LOG}"; then
            return 0
        fi
        if ! kill -0 "${WORKER_PID}" 2>/dev/null; then
            wait "${WORKER_PID}" || true
            fail "Worker exited before expected log: ${pattern}"
        fi
        sleep 1
    done

    fail "Timed out waiting for worker log: ${pattern}"
}

printf 'DLQ integration test\n'
printf 'Source topic: %s\nDLQ topic:    %s\nGroup:        %s\n' \
    "${SOURCE_TOPIC}" "${DLQ_TOPIC}" "${GROUP_ID}"

docker inspect "${KAFKA_CONTAINER}" >/dev/null 2>&1 ||
    fail "Kafka container '${KAFKA_CONTAINER}' is not available"

create_topic "${SOURCE_TOPIC}"
create_topic "${DLQ_TOPIC}"
wait_for_topic "${SOURCE_TOPIC}"
wait_for_topic "${DLQ_TOPIC}"

# Offset 0 deliberately contains invalid JSON.
publish_raw_message "${SOURCE_TOPIC}" '{invalid-json'
printf 'Published malformed test message.\n'

cd "${ROOT_DIR}/rust-worker"

cargo build --quiet

WORKER_BIN="${ROOT_DIR}/rust-worker/target/debug/rust-worker"
[[ -x "${WORKER_BIN}" ]] || fail "Compiled worker binary not found"

# First attempt: invalid destination. The worker must exit with an error,
# and the consumer group must not commit source offset 1.
set +e
KAFKA_BROKERS="${BOOTSTRAP}" \
KAFKA_TOPIC="${SOURCE_TOPIC}" \
KAFKA_GROUP_ID="${GROUP_ID}" \
KAFKA_DLQ_TOPIC="${INVALID_DLQ}" \
RUST_LOG=info \
timeout 20s "${WORKER_BIN}" >"${WORKER_LOG}" 2>&1
FAILURE_STATUS=$?
set -e

if [[ "${FAILURE_STATUS}" -eq 0 || "${FAILURE_STATUS}" -eq 124 ]]; then
    fail "Expected the invalid-DLQ worker to fail promptly; exit status was ${FAILURE_STATUS}"
fi

grep -Fq "DLQ publication failed; source message will not be committed" "${WORKER_LOG}" ||
    fail "Expected structured DLQ failure log was not found"

GROUP_OUTPUT="$(group_description)"
if awk -v topic="${SOURCE_TOPIC}" \
    '$2 == topic && $4 == "1" { found = 1 }
     END { exit !found }' <<<"${GROUP_OUTPUT}"; then
    fail "Consumer group appears to have committed past the unhandled source message"
fi

printf 'Verified: invalid DLQ destination fails without advancing the source offset.\n'

# Second attempt: same group and source topic, now using a valid DLQ topic.
: >"${WORKER_LOG}"
KAFKA_BROKERS="${BOOTSTRAP}" \
KAFKA_TOPIC="${SOURCE_TOPIC}" \
KAFKA_GROUP_ID="${GROUP_ID}" \
KAFKA_DLQ_TOPIC="${DLQ_TOPIC}" \
RUST_LOG=info \
"${WORKER_BIN}" >"${WORKER_LOG}" 2>&1 &
WORKER_PID=$!

wait_for_worker_log "Rejected message published to dead-letter queue"
wait_for_committed_offset

# Stop the worker once its successful DLQ publish and source commit are observed.
kill "${WORKER_PID}" 2>/dev/null || true
wait "${WORKER_PID}" 2>/dev/null || true
WORKER_PID=""

DLQ_OUTPUT="$(
    kafka_cli "${KAFKA_BIN}/kafka-console-consumer.sh" \
        --bootstrap-server localhost:9092 \
        --topic "${DLQ_TOPIC}" \
        --from-beginning --max-messages 1 \
        --timeout-ms 10000 2>/dev/null
)"

python3 - "${SOURCE_TOPIC}" "${DLQ_OUTPUT}" <<'PY'
import json
import sys

expected_topic = sys.argv[1]
raw = sys.argv[2]

try:
    envelope = json.loads(raw)
except json.JSONDecodeError as exc:
    raise SystemExit(f"FAIL: DLQ record was not valid JSON: {exc}")

assert envelope.get("source_topic") == expected_topic, envelope
assert envelope.get("source_partition") == 0, envelope
assert envelope.get("source_offset") == 0, envelope
assert envelope.get("rejection_reason", "").startswith(
    "Invalid delivery task JSON"
), envelope
assert envelope.get("original_payload") == list(b"{invalid-json"), envelope

print("Verified DLQ envelope metadata and original payload bytes.")
PY

printf '\nPASS: DLQ failure/recovery integration test completed.\n'
printf 'Test source topic: %s\n' "${SOURCE_TOPIC}"
printf 'Test DLQ topic:    %s\n' "${DLQ_TOPIC}"
printf 'Test group:        %s\n' "${GROUP_ID}"
printf 'Test topics are retained for inspection; no topics were deleted.\n'
