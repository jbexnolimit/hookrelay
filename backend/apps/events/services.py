import json
import os
import threading

from kafka import KafkaProducer

_producer = None
_producer_lock = threading.Lock()


def get_kafka_producer():
    global _producer

    with _producer_lock:
        if _producer is None:
            _producer = KafkaProducer(
                bootstrap_servers=os.getenv(
                    "KAFKA_BOOTSTRAP_SERVERS",
                    "127.0.0.1:9092",
                ),
                key_serializer=lambda key: str(key).encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                retries=5,
                request_timeout_ms=10000,
                max_block_ms=5000,
                client_id="hookrelay-django-producer",
            )

    return _producer
