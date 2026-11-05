from django.test import TestCase, override_settings
from rest_framework.test import APIClient

from .models import RecoveryReceiverConfig


@override_settings(ROOT_URLCONF="apps.devtools.test_urls")
class RecoveryReceiverTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.config = RecoveryReceiverConfig.objects.create(
            key="default",
            failures_before_success=2,
            attempts_received=0,
        )
        self.url = "/api/dev/receiver/recover/"

    def test_fails_twice_then_succeeds(self):
        first = self.client.post(
            self.url, {"event": "test"}, format="json"
        )
        second = self.client.post(
            self.url, {"event": "test"}, format="json"
        )
        third = self.client.post(
            self.url, {"event": "test"}, format="json"
        )

        self.assertEqual(first.status_code, 500)
        self.assertEqual(first.data["attempt"], 1)

        self.assertEqual(second.status_code, 500)
        self.assertEqual(second.data["attempt"], 2)

        self.assertEqual(third.status_code, 200)
        self.assertEqual(third.data["attempt"], 3)
        self.assertTrue(third.data["received"])

        self.config.refresh_from_db()
        self.assertEqual(self.config.attempts_received, 3)

    def test_zero_failures_succeeds_immediately(self):
        self.config.failures_before_success = 0
        self.config.save(update_fields=["failures_before_success"])

        response = self.client.post(
            self.url, {"event": "test"}, format="json"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["attempt"], 1)

    def test_existing_success_receiver_still_succeeds(self):
        response = self.client.post(
            "/api/dev/receiver/success/",
            {"event": "test"},
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data["received"])

    def test_existing_failure_receiver_still_fails(self):
        response = self.client.post(
            "/api/dev/receiver/fail/",
            {"event": "test"},
            format="json",
        )

        self.assertEqual(response.status_code, 500)
        self.assertFalse(response.data["received"])
