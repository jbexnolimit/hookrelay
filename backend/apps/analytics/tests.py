
from django.test import TestCase
from rest_framework.test import APIRequestFactory

from apps.analytics.views import AnalyticsSummaryView
from apps.deliveries.models import Delivery, DeliveryAttempt
from apps.events.models import Event
from apps.webhooks.models import WebhookEndpoint


class AnalyticsSummaryTests(TestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        self.url = "/api/analytics/summary/"

        self.endpoint = WebhookEndpoint.objects.create(
            name="Analytics Test Receiver",
            slug="analytics-test",
            url="http://localhost/test-receiver/",
        )

    def create_delivery(self, event_type, status):
        event = Event.objects.create(
            event_type=event_type,
            payload={},
        )
        return Delivery.objects.create(
            event=event,
            endpoint=self.endpoint,
            status=status,
        )

    def get_summary(self):
        request = self.factory.get(self.url)
        response = AnalyticsSummaryView.as_view()(request)
        self.assertEqual(response.status_code, 200)
        return response.data

    def test_success_rate_includes_queued_deliveries(self):
        self.create_delivery(
            "order.created",
            Delivery.Status.DELIVERED,
        )
        self.create_delivery(
            "payment.failed",
            Delivery.Status.QUEUED,
        )

        summary = self.get_summary()

        self.assertEqual(summary["total_deliveries"], 2)
        self.assertEqual(summary["success_rate_percent"], 50.0)

    def test_success_rate_includes_failed_deliveries(self):
        self.create_delivery(
            "order.created",
            Delivery.Status.DELIVERED,
        )
        self.create_delivery(
            "payment.failed",
            Delivery.Status.FAILED,
        )

        summary = self.get_summary()

        self.assertEqual(summary["success_rate_percent"], 50.0)

    def test_success_rate_is_zero_without_deliveries(self):
        summary = self.get_summary()

        self.assertEqual(summary["total_deliveries"], 0)
        self.assertEqual(summary["success_rate_percent"], 0)

    def test_average_response_time_uses_recorded_attempts(self):
        delivery = self.create_delivery(
            "order.created",
            Delivery.Status.DELIVERED,
        )

        DeliveryAttempt.objects.create(
            delivery=delivery,
            attempt_number=1,
            result=DeliveryAttempt.Result.SUCCESS,
            http_status_code=200,
            response_time_ms=40,
        )
        DeliveryAttempt.objects.create(
            delivery=delivery,
            attempt_number=2,
            result=DeliveryAttempt.Result.FAILURE,
            http_status_code=500,
            response_time_ms=60,
        )

        summary = self.get_summary()

        self.assertEqual(summary["average_response_time"], 50.0)
