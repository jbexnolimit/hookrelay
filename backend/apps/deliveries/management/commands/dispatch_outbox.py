"""Publish durable outbox entries to Kafka."""

import logging
import os
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from apps.deliveries.models import Delivery, OutboxMessage
from apps.events.services import get_kafka_producer

logger = logging.getLogger(__name__)

DISPATCH_LEASE_SECONDS = 60
MAX_BACKOFF_SECONDS = 900


def publish_outbox_payload(task_payload):
    """Publish a previously persisted task snapshot and await Kafka ack."""
    topic = os.getenv(
        "KAFKA_DELIVERY_TOPIC",
        "hookrelay.delivery-tasks",
    )
    metadata = get_kafka_producer().send(
        topic,
        key=str(task_payload["delivery_id"]),
        value=task_payload,
    ).get(timeout=15)

    logger.info(
        "Published outbox task: delivery_id=%s topic=%s partition=%s offset=%s",
        task_payload["delivery_id"],
        metadata.topic,
        metadata.partition,
        metadata.offset,
    )
    return metadata


class Command(BaseCommand):
    help = "Publish pending transactional outbox entries to Kafka."

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Maximum outbox entries to inspect per run.",
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]

        if batch_size < 1:
            self.stderr.write("--batch-size must be at least 1.")
            return

        published = 0
        failed = 0
        superseded = 0
        inspected = 0

        while inspected < batch_size:
            now = timezone.now()

            # Move the next-attempt time forward before releasing the lock.
            # This acts as a lease, allowing recovery if this process crashes.
            with transaction.atomic():
                message = (
                    OutboxMessage.objects.select_for_update(skip_locked=True)
                    .filter(
                        status=OutboxMessage.Status.PENDING,
                        next_attempt_at__lte=now,
                    )
                    .select_related("delivery")
                    .order_by("next_attempt_at", "id")
                    .first()
                )

                if message is None:
                    break

                delivery = Delivery.objects.filter(
                    pk=message.delivery_id,
                    status=Delivery.Status.PROCESSING,
                    claim_token=message.claim_token,
                ).exists()

                if not delivery:
                    message.status = OutboxMessage.Status.SUPERSEDED
                    message.last_error = (
                        "Delivery claim is no longer current."
                    )
                    message.save(update_fields=["status", "last_error"])
                    superseded += 1
                    inspected += 1
                    continue

                message.publish_attempts += 1
                message.next_attempt_at = now + timedelta(
                    seconds=DISPATCH_LEASE_SECONDS
                )
                message.save(
                    update_fields=["publish_attempts", "next_attempt_at"]
                )

                message_id = message.pk
                claim_token = message.claim_token
                task_payload = message.task_payload
                attempt_number = message.publish_attempts

            inspected += 1

            try:
                publish_outbox_payload(task_payload)
            except Exception as exc:
                delay = min(
                    30 * (2 ** min(attempt_number - 1, 10)),
                    MAX_BACKOFF_SECONDS,
                )
                retry_at = timezone.now() + timedelta(seconds=delay)

                updated = OutboxMessage.objects.filter(
                    pk=message_id,
                    status=OutboxMessage.Status.PENDING,
                    claim_token=claim_token,
                ).update(
                    next_attempt_at=retry_at,
                    last_error=str(exc)[:4000],
                )

                logger.exception(
                    "Outbox publication failed: outbox_id=%s delivery_id=%s",
                    message_id,
                    task_payload["delivery_id"],
                )
                failed += 1
                self.stderr.write(
                    f"Outbox {message_id} publication failed; "
                    f"retry scheduled in {delay}s"
                    if updated
                    else f"Outbox {message_id} changed before failure update."
                )
            else:
                updated = OutboxMessage.objects.filter(
                    pk=message_id,
                    status=OutboxMessage.Status.PENDING,
                    claim_token=claim_token,
                ).update(
                    status=OutboxMessage.Status.PUBLISHED,
                    published_at=timezone.now(),
                    last_error="",
                )

                if updated:
                    published += 1
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Published outbox {message_id} "
                            f"for delivery {task_payload['delivery_id']}"
                        )
                    )

        self.stdout.write(
            f"Outbox dispatch finished: published={published}, "
            f"failed={failed}, superseded={superseded}, inspected={inspected}"
        )
