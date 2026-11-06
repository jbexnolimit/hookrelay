import uuid
from unittest.mock import patch

from django.test import TestCase
from rest_framework.test import APIClient

from apps.deliveries.models import Delivery
from apps.events.models import Event
from apps.webhooks.models import WebhookEndpoint


class EventPublishingClaimTokenTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.endpoint = WebhookEndpoint.objects.create(
            slug="orders-test",
            name="Orders Test Endpoint",
            url="https://example.com/webhooks/orders",
            event_types=["order.created"],
            is_active=True,
        )
        self.payload = {
            "event_type": "order.created",
            "payload": {"order_id": "ORD-1001"},
        }

    @patch("apps.events.views.publish_delivery_task")
    def test_successful_publish_uses_active_claim_token(self, publish_task):
        response = self.client.post(
            "/api/events/",
            self.payload,
            format="json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(Event.objects.count(), 1)

        delivery = Delivery.objects.get(endpoint=self.endpoint)

        self.assertEqual(delivery.status, Delivery.Status.PROCESSING)
        self.assertIsNotNone(delivery.claim_token)

        publish_task.assert_called_once()
        published_delivery = publish_task.call_args.args[0]
        self.assertEqual(published_delivery.pk, delivery.pk)
        self.assertEqual(published_delivery.status, Delivery.Status.PROCESSING)
        self.assertEqual(published_delivery.claim_token, delivery.claim_token)

    @patch(
        "apps.events.views.publish_delivery_task",
        side_effect=RuntimeError("Kafka unavailable"),
    )
    def test_publish_failure_requeues_delivery_and_clears_claim(
        self,
        publish_task,
    ):
        response = self.client.post(
            "/api/events/",
            self.payload,
            format="json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertIn("kafka_publish_failures", response.data)

        delivery = Delivery.objects.get(endpoint=self.endpoint)

        self.assertEqual(delivery.status, Delivery.Status.QUEUED)
        self.assertIsNone(delivery.claim_token)
        self.assertIsNotNone(delivery.next_retry_at)
        publish_task.assert_called_once()

    @patch("apps.events.views.publish_delivery_task")
    def test_publish_failure_does_not_overwrite_newer_claim(
        self,
        publish_task,
    ):
        newer_token = uuid.uuid4()

        def change_claim_then_fail(delivery):
            # Simulate another process replacing the claim before the
            # original publish operation reports failure.
            Delivery.objects.filter(pk=delivery.pk).update(
                claim_token=newer_token,
            )
            raise RuntimeError("Kafka unavailable")

        publish_task.side_effect = change_claim_then_fail

        response = self.client.post(
            "/api/events/",
            self.payload,
            format="json",
        )

        self.assertEqual(response.status_code, 201)

        delivery = Delivery.objects.get(endpoint=self.endpoint)

        self.assertEqual(delivery.status, Delivery.Status.PROCESSING)
        self.assertEqual(delivery.claim_token, newer_token)
        self.assertIsNone(delivery.next_retry_at)
        self.assertIn(
            "preserved any newer delivery claims",
            response.data["message"],
        )
        self.assertIn(delivery.pk, response.data["kafka_publish_failures"])
        publish_task.assert_called_once()
