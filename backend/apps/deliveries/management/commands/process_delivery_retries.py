import logging
import uuid
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import F
from django.utils import timezone

from apps.deliveries.models import Delivery
from apps.events.services import publish_delivery_task

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Publish due webhook retries and recover stale deliveries."

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Maximum number of retries to process per run.",
        )
        parser.add_argument(
            "--stale-after-seconds",
            type=int,
            default=300,
            help="Seconds before a processing delivery is considered stale.",
        )

    def handle(self, *args, **options):
        batch_size = options["batch_size"]
        stale_seconds = options["stale_after_seconds"]

        if batch_size < 1:
            self.stderr.write("--batch-size must be at least 1.")
            return

        if stale_seconds < 1:
            self.stderr.write("--stale-after-seconds must be at least 1.")
            return

        now = timezone.now()
        stale_before = now - timedelta(seconds=stale_seconds)

        # Recover old queued deliveries that have no retry timestamp.
        # These may have been left behind without a published Kafka task.
        stale_queued = Delivery.objects.filter(
            status=Delivery.Status.QUEUED,
            next_retry_at__isnull=True,
            updated_at__lt=stale_before,
        )

        queued_recoverable = stale_queued.filter(
            attempt_count__lt=F("max_attempts")
        ).update(
            next_retry_at=now,
            updated_at=now,
        )

        queued_exhausted = stale_queued.filter(
            attempt_count__gte=F("max_attempts")
        ).update(
            status=Delivery.Status.FAILED,
            next_retry_at=None,
            claim_token=None,
            last_error="Queued delivery exceeded maximum attempts.",
            updated_at=now,
        )

        if queued_recoverable or queued_exhausted:
            self.stdout.write(
                "Stale queued recovery: "
                f"requeued={queued_recoverable}, "
                f"marked_failed={queued_exhausted}"
            )

        # Recover deliveries that may have been abandoned during publishing
        # or worker processing. Recovery can result in duplicate delivery.
        stale_deliveries = Delivery.objects.filter(
            status=Delivery.Status.PROCESSING,
            updated_at__lt=stale_before,
        )

        recoverable = stale_deliveries.filter(
            attempt_count__lt=F("max_attempts"),
        ).update(
            status=Delivery.Status.QUEUED,
            next_retry_at=now,
            claim_token=None,
            last_error="Recovered stale processing delivery.",
            updated_at=now,
        )

        exhausted = stale_deliveries.filter(
            attempt_count__gte=F("max_attempts"),
        ).update(
            status=Delivery.Status.FAILED,
            next_retry_at=None,
            claim_token=None,
            last_error="Processing timed out after maximum attempts.",
            updated_at=now,
        )

        if recoverable or exhausted:
            self.stdout.write(
                f"Stale recovery: requeued={recoverable}, "
                f"marked_failed={exhausted}"
            )

        due_ids = list(
            Delivery.objects.filter(
                status=Delivery.Status.QUEUED,
                next_retry_at__lte=timezone.now(),
                attempt_count__lt=F("max_attempts"),
            )
            .order_by("next_retry_at", "id")
            .values_list("id", flat=True)[:batch_size]
        )

        published = 0
        failed = 0

        for delivery_id in due_ids:
            with transaction.atomic():
                delivery = (
                    Delivery.objects.select_for_update()
                    .filter(
                        pk=delivery_id,
                        status=Delivery.Status.QUEUED,
                        next_retry_at__lte=timezone.now(),
                        attempt_count__lt=F("max_attempts"),
                    )
                    .select_related("event", "endpoint")
                    .first()
                )

                if delivery is None:
                    continue

                claim_token = uuid.uuid4()
                delivery.status = Delivery.Status.PROCESSING
                delivery.claim_token = claim_token
                delivery.save(
                    update_fields=[
                        "status",
                        "claim_token",
                        "updated_at",
                    ]
                )

            try:
                publish_delivery_task(delivery)
            except Exception as exc:
                logger.exception(
                    "Failed to publish retry for delivery_id=%s",
                    delivery_id,
                )

                Delivery.objects.filter(
                    pk=delivery_id,
                    status=Delivery.Status.PROCESSING,
                    claim_token=claim_token,
                ).update(
                    status=Delivery.Status.QUEUED,
                    claim_token=None,
                    next_retry_at=timezone.now() + timedelta(seconds=30),
                    updated_at=timezone.now(),
                )

                failed += 1
                self.stderr.write(
                    f"Could not publish delivery {delivery_id}: {exc}"
                )
            else:
                published += 1
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Published retry for delivery {delivery_id}"
                    )
                )

        self.stdout.write(
            f"Retry scheduler finished: published={published}, "
            f"publish_failures={failed}, candidates={len(due_ids)}"
        )
