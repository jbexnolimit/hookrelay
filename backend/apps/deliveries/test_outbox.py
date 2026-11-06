import uuid
from datetime import timedelta
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from apps.deliveries.models import Delivery, OutboxMessage
from apps.deliveries.outbox import enqueue_delivery_task
from apps.events.models import Event
from apps.webhooks.models import WebhookEndpoint


class OutboxDispatchTests(TestCase):
    def setUp(self):
        self.event = Event.objects.create(
            event_type="order.created",
            payload={"order_ref": "ORD-OUTBOX-TEST"},
        )
        self.endpoint = WebhookEndpoint.objects.create(
            slug="outbox-test",
            name="Outbox Test",
            url="https://example.com/webhooks/orders",
            event_types=["order.created"],
            is_active=True,
        )
        self.claim_token = uuid.uuid4()
        self.delivery = Delivery.objects.create(
            event=self.event,
            endpoint=self.endpoint,
            status=Delivery.Status.PROCESSING,
            claim_token=self.claim_token,
        )

        self.outbox = enqueue_delivery_task(
            self.delivery,
            self.claim_token,
        )

    @patch("apps.deliveries.management.commands.dispatch_outbox.publish_outbox_payload")
    def test_successful_publication_marks_entry_published(self, publish_mock):
        call_command("dispatch_outbox")

        self.outbox.refresh_from_db()
        self.assertEqual(self.outbox.status, OutboxMessage.Status.PUBLISHED)
        self.assertIsNotNone(self.outbox.published_at)
        self.assertEqual(self.outbox.publish_attempts, 1)
        publish_mock.assert_called_once_with(self.outbox.task_payload)

    @patch(
        "apps.deliveries.management.commands.dispatch_outbox.publish_outbox_payload",
        side_effect=RuntimeError("Kafka unavailable"),
    )
    def test_publication_failure_remains_pending_with_backoff(self, publish_mock):
        before = timezone.now()

        call_command("dispatch_outbox")

        self.outbox.refresh_from_db()
        self.assertEqual(self.outbox.status, OutboxMessage.Status.PENDING)
        self.assertIsNone(self.outbox.published_at)
        self.assertEqual(self.outbox.publish_attempts, 1)
        self.assertEqual(self.outbox.last_error, "Kafka unavailable")
        self.assertGreaterEqual(
            self.outbox.next_attempt_at,
            before + timedelta(seconds=29),
        )
        self.assertLessEqual(
            self.outbox.next_attempt_at,
            timezone.now() + timedelta(seconds=31),
        )
        publish_mock.assert_called_once()

    @patch("apps.deliveries.management.commands.dispatch_outbox.publish_outbox_payload")
    def test_obsolete_claim_is_superseded_without_publication(self, publish_mock):
        Delivery.objects.filter(pk=self.delivery.pk).update(
            claim_token=uuid.uuid4(),
        )

        call_command("dispatch_outbox")

        self.outbox.refresh_from_db()
        self.assertEqual(self.outbox.status, OutboxMessage.Status.SUPERSEDED)
        self.assertEqual(
            self.outbox.last_error,
            "Delivery claim is no longer current.",
        )
        publish_mock.assert_not_called()

    def test_enqueue_requires_a_current_processing_claim(self):
        self.delivery.status = Delivery.Status.QUEUED
        self.delivery.save(update_fields=["status"])

        with self.assertRaisesMessage(
            ValueError,
            "The delivery must be processing under the supplied claim token.",
        ):
            enqueue_delivery_task(self.delivery, self.claim_token)

        self.assertEqual(
            OutboxMessage.objects.filter(delivery=self.delivery).count(),
            1,
        )

    def test_duplicate_outbox_entry_for_same_claim_is_rejected(self):
        from django.db import IntegrityError, transaction

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                enqueue_delivery_task(self.delivery, self.claim_token)
