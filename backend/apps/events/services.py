import json
import logging
import os
import threading

from kafka import KafkaProducer

logger = logging.getLogger(__name__)

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


def publish_delivery_task(delivery):
    """Publish a task built from a saved Delivery and its related records."""
    topic = os.getenv(
        "KAFKA_DELIVERY_TOPIC",
        "hookrelay-delivery-tasks",
    )

    task = {
        "delivery_id": delivery.pk,
        "event_id": delivery.event_id,
        "event_type": delivery.event.event_type,
        "payload": delivery.event.payload,
        "webhook_url": delivery.endpoint.url,
    }

    producer = get_kafka_producer()

    # Wait for Kafka's acknowledgement before reporting success.
    metadata = producer.send(
        topic,
        key=str(delivery.pk),
        value=task,
    ).get(timeout=15)

    logger.info(
        "Published delivery task: delivery_id=%s topic=%s partition=%s offset=%s",
        delivery.pk,
        metadata.topic,
        metadata.partition,
        metadata.offset,
    )

    return metadata