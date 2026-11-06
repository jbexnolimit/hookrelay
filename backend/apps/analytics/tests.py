
from datetime import timedelta

from django.test import TestCase
from django.utils import timezone
from rest_framework.test import APIRequestFactory

from apps.analytics.views import AnalyticsSummaryView, DeliveryActivityView
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


class DeliveryActivityTests(TestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        self.url = "/api/analytics/activity/"

        self.endpoint = WebhookEndpoint.objects.create(
            name="Activity Test Receiver",
            slug="activity-test",
            url="http://localhost/activity-test/",
        )

    def create_delivery(self, event_type, status, days_ago=0):
        event = Event.objects.create(
            event_type=event_type,
            payload={},
        )
        delivery = Delivery.objects.create(
            event=event,
            endpoint=self.endpoint,
            status=status,
        )

        created_at = timezone.now() - timedelta(days=days_ago)
        Delivery.objects.filter(pk=delivery.pk).update(
            created_at=created_at,
        )

        return delivery

    def get_activity(self, days=None):
        query = {} if days is None else {"days": days}
        request = self.factory.get(self.url, query)
        response = DeliveryActivityView.as_view()(request)
        return response

    def test_activity_returns_seven_days(self):
        response = self.get_activity()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.data["days"]), 7)
        self.assertEqual(
            [day["date"] for day in response.data["days"]],
            sorted(day["date"] for day in response.data["days"]),
        )

    def test_activity_supports_fourteen_and_thirty_days(self):
        for requested_days in (14, 30):
            with self.subTest(days=requested_days):
                response = self.get_activity(days=requested_days)

                self.assertEqual(response.status_code, 200)
                self.assertEqual(len(response.data["days"]), requested_days)

    def test_activity_rejects_unsupported_day_ranges(self):
        for requested_days in ("0", "8", "90", "invalid"):
            with self.subTest(days=requested_days):
                response = self.get_activity(days=requested_days)

                self.assertEqual(response.status_code, 400)
                self.assertIn("detail", response.data)

    def test_activity_counts_deliveries_by_current_status(self):
        self.create_delivery(
            "order.created",
            Delivery.Status.DELIVERED,
        )
        self.create_delivery(
            "payment.failed",
            Delivery.Status.FAILED,
        )
        self.create_delivery(
            "order.updated",
            Delivery.Status.QUEUED,
        )
        self.create_delivery(
            "order.processing",
            Delivery.Status.PROCESSING,
        )

        response = self.get_activity()
        self.assertEqual(response.status_code, 200)
        today = timezone.localdate().isoformat()
        today_data = next(
            day for day in response.data["days"]
            if day["date"] == today
        )

        self.assertEqual(today_data["delivered"], 1)
        self.assertEqual(today_data["failed"], 1)
        self.assertEqual(today_data["queued"], 1)
        self.assertEqual(today_data["processing"], 1)

    def test_activity_uses_zero_counts_for_empty_days(self):
        response = self.get_activity()
        self.assertEqual(response.status_code, 200)

        for day in response.data["days"]:
            self.assertEqual(day["queued"], 0)
            self.assertEqual(day["processing"], 0)
            self.assertEqual(day["delivered"], 0)
            self.assertEqual(day["failed"], 0)

    def test_activity_excludes_deliveries_older_than_seven_days(self):
        self.create_delivery(
            "order.created",
            Delivery.Status.DELIVERED,
            days_ago=7,
        )

        response = self.get_activity()
        self.assertEqual(response.status_code, 200)

        self.assertTrue(
            all(day["delivered"] == 0 for day in response.data["days"])
        )
