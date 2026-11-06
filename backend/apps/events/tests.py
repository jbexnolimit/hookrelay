from django.test import TestCase
from rest_framework.test import APIClient

from apps.deliveries.models import Delivery, OutboxMessage
from apps.events.models import Event
from apps.webhooks.models import WebhookEndpoint


class EventOutboxTests(TestCase):
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

    def test_event_creation_atomically_queues_delivery_task(self):
        response = self.client.post(
            "/api/events/",
            self.payload,
            format="json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(Event.objects.count(), 1)
        self.assertIn("queued for publication", response.data["message"])

        delivery = Delivery.objects.get(endpoint=self.endpoint)
        self.assertEqual(delivery.status, Delivery.Status.PROCESSING)
        self.assertIsNotNone(delivery.claim_token)

        outbox = OutboxMessage.objects.get(delivery=delivery)
        self.assertEqual(outbox.status, OutboxMessage.Status.PENDING)
        self.assertEqual(outbox.claim_token, delivery.claim_token)
        self.assertEqual(
            outbox.task_payload["delivery_id"],
            delivery.pk,
        )
        self.assertEqual(
            outbox.task_payload["claim_token"],
            str(delivery.claim_token),
        )
        self.assertEqual(
            outbox.task_payload["event_type"],
            "order.created",
        )
        self.assertEqual(
            outbox.task_payload["payload"],
            {"order_id": "ORD-1001"},
        )
        self.assertEqual(
            outbox.task_payload["webhook_url"],
            self.endpoint.url,
        )

    def test_idempotent_replay_does_not_create_duplicate_outbox(self):
        payload = {
            **self.payload,
            "idempotency_key": "event-outbox-test-001",
        }

        first_response = self.client.post(
            "/api/events/",
            payload,
            format="json",
        )
        second_response = self.client.post(
            "/api/events/",
            payload,
            format="json",
        )

        self.assertEqual(first_response.status_code, 201)
        self.assertEqual(second_response.status_code, 200)
        self.assertTrue(second_response.data["idempotent_replay"])
        self.assertEqual(Event.objects.count(), 1)
        self.assertEqual(Delivery.objects.count(), 1)
        self.assertEqual(OutboxMessage.objects.count(), 1)

    def test_event_without_matching_endpoint_creates_no_outbox(self):
        payload = {
            "event_type": "invoice.created",
            "payload": {"invoice_id": "INV-1001"},
        }

        response = self.client.post(
            "/api/events/",
            payload,
            format="json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(Delivery.objects.count(), 0)
        self.assertEqual(OutboxMessage.objects.count(), 0)
        self.assertIn("no active webhook endpoint", response.data["message"])
