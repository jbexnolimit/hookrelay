from django.test import TestCase
from rest_framework.test import APIClient

from apps.webhooks.models import WebhookEndpoint


class WebhookEndpointURLValidationTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.payload = {
            "slug": "url-validation-test",
            "name": "URL Validation Test",
            "url": "https://example.com/webhooks/orders",
            "event_types": ["order.created"],
            "is_active": True,
        }

    def test_accepts_https_destination(self):
        response = self.client.post(
            "/api/webhooks/",
            self.payload,
            format="json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(WebhookEndpoint.objects.count(), 1)

    def test_accepts_local_http_destination(self):
        payload = {
            **self.payload,
            "url": "http://127.0.0.1:8001/api/dev/receiver/",
        }

        response = self.client.post(
            "/api/webhooks/",
            payload,
            format="json",
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(WebhookEndpoint.objects.count(), 1)

    def test_rejects_ftp_destination(self):
        payload = {
            **self.payload,
            "url": "ftp://example.com/webhooks/orders",
        }

        response = self.client.post(
            "/api/webhooks/",
            payload,
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("url", response.data)
        self.assertEqual(WebhookEndpoint.objects.count(), 0)
