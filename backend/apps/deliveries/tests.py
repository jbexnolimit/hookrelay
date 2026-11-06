from datetime import timedelta
from types import SimpleNamespace
from django.test import SimpleTestCase
from django.utils import timezone
from apps.deliveries.models import Delivery, DeliveryAttempt, OutboxMessage
from apps.deliveries.retry import (
    RETRY_DELAYS_SECONDS,
    is_retryable_failure,
    schedule_retry,
)
from unittest.mock import patch, call
from django.core.management import call_command
from django.test import TestCase
from apps.events.models import Event
from apps.webhooks.models import WebhookEndpoint
from rest_framework.test import APIClient
import uuid



class RetryabilityTests(SimpleTestCase):
    def test_network_error_is_retryable(self):
        self.assertTrue(is_retryable_failure(None))

    def test_timeout_and_rate_limit_are_retryable(self):
        self.assertTrue(is_retryable_failure(408))
        self.assertTrue(is_retryable_failure(429))

    def test_server_errors_are_retryable(self):
        for status_code in (500, 502, 503, 599):
            with self.subTest(status_code=status_code):
                self.assertTrue(is_retryable_failure(status_code))

    def test_client_errors_are_not_retryable(self):
        for status_code in (400, 401, 403, 404, 422):
            with self.subTest(status_code=status_code):
                self.assertFalse(is_retryable_failure(status_code))


class RetrySchedulingTests(SimpleTestCase):
    def make_delivery(self, attempt_count, max_attempts=5):
        return SimpleNamespace(
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            status=Delivery.Status.FAILED,
            next_retry_at=None,
        )

    def test_retry_delays_use_exponential_backoff(self):
        for attempt_number, expected_delay in enumerate(
            RETRY_DELAYS_SECONDS, start=1
        ):
            with self.subTest(attempt_number=attempt_number):
                delivery = self.make_delivery(attempt_number)

                before = timezone.now()
                scheduled = schedule_retry(delivery, 503)
                after = timezone.now()

                self.assertTrue(scheduled)
                self.assertEqual(delivery.status, Delivery.Status.QUEUED)
                self.assertGreaterEqual(
                    delivery.next_retry_at,
                    before + timedelta(seconds=expected_delay),
                )
                self.assertLessEqual(
                    delivery.next_retry_at,
                    after + timedelta(seconds=expected_delay),
                )

    def test_permanent_failure_is_not_retried(self):
        delivery = self.make_delivery(attempt_count=1)

        scheduled = schedule_retry(delivery, 422)

        self.assertFalse(scheduled)
        self.assertEqual(delivery.status, Delivery.Status.FAILED)
        self.assertIsNone(delivery.next_retry_at)

    def test_maximum_attempts_stops_retries(self):
        delivery = self.make_delivery(attempt_count=5)

        scheduled = schedule_retry(delivery, 503)

        self.assertFalse(scheduled)
        self.assertEqual(delivery.status, Delivery.Status.FAILED)
        self.assertIsNone(delivery.next_retry_at)

    def test_custom_attempt_limit_is_respected(self):
        delivery = self.make_delivery(attempt_count=2, max_attempts=2)

        scheduled = schedule_retry(delivery, 503)

        self.assertFalse(scheduled)
        self.assertEqual(delivery.status, Delivery.Status.FAILED)
        self.assertIsNone(delivery.next_retry_at)




class DeliveryRetryCommandTests(TestCase):
    def setUp(self):
        event = Event.objects.create(
            event_type="order.created",
            payload={
                "order_ref": "ORD-RETRY-TEST",
                "amount_minor": 200000,
                "currency": "PHP",
                "status": "placed",
            },
        )
        endpoint = WebhookEndpoint.objects.create(
            slug="retry-test",
            name="Retry Test",
            url="http://127.0.0.1:8002/api/v1/webhooks/hookrelay",
            event_types=["order.created"],
            is_active=True,
        )
        self.delivery = Delivery.objects.create(
            event=event,
            endpoint=endpoint,
            status=Delivery.Status.QUEUED,
            attempt_count=1,
            max_attempts=5,
            next_retry_at=timezone.now() - timedelta(seconds=1),
        )

    def run_processor(self, **options):
        defaults = {"stale_after_seconds": 300}
        defaults.update(options)
        call_command("process_delivery_retries", **defaults)

    def test_due_retry_is_queued_and_claimed(self):
        self.run_processor()

        self.delivery.refresh_from_db()
        self.assertEqual(
            self.delivery.status,
            Delivery.Status.PROCESSING,
        )
        self.assertIsNotNone(self.delivery.claim_token)

        outbox = OutboxMessage.objects.get(delivery=self.delivery)
        self.assertEqual(outbox.status, OutboxMessage.Status.PENDING)
        self.assertEqual(outbox.claim_token, self.delivery.claim_token)
        self.assertEqual(
            outbox.task_payload["delivery_id"],
            self.delivery.pk,
        )

    def test_outbox_creation_failure_rolls_back_retry_claim(self):
        with patch(
            "apps.deliveries.management.commands.process_delivery_retries."
            "enqueue_delivery_task",
            side_effect=RuntimeError("Database unavailable"),
        ):
            self.run_processor()

        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.status, Delivery.Status.QUEUED)
        self.assertIsNone(self.delivery.claim_token)
        self.assertEqual(OutboxMessage.objects.count(), 0)

    def test_future_retry_is_not_queued(self):
        self.delivery.next_retry_at = timezone.now() + timedelta(hours=1)
        self.delivery.save(update_fields=["next_retry_at"])

        self.run_processor()

        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.status, Delivery.Status.QUEUED)
        self.assertEqual(OutboxMessage.objects.count(), 0)

    def test_stale_queued_delivery_without_retry_time_is_recovered(self):
        self.delivery.next_retry_at = None
        self.delivery.save(update_fields=["next_retry_at"])

        Delivery.objects.filter(pk=self.delivery.pk).update(
            updated_at=timezone.now() - timedelta(minutes=10)
        )

        self.run_processor()

        self.delivery.refresh_from_db()
        self.assertEqual(
            self.delivery.status,
            Delivery.Status.PROCESSING,
        )
        self.assertEqual(self.delivery.attempt_count, 1)
        self.assertIsNotNone(self.delivery.claim_token)
        self.assertEqual(
            OutboxMessage.objects.get(delivery=self.delivery).status,
            OutboxMessage.Status.PENDING,
        )

    def test_stale_processing_delivery_is_recovered(self):
        self.delivery.status = Delivery.Status.PROCESSING
        self.delivery.save(update_fields=["status"])

        Delivery.objects.filter(pk=self.delivery.pk).update(
            updated_at=timezone.now() - timedelta(minutes=10)
        )

        self.run_processor()

        self.delivery.refresh_from_db()
        self.assertEqual(
            self.delivery.status,
            Delivery.Status.PROCESSING,
        )
        self.assertEqual(
            self.delivery.last_error,
            "Recovered stale processing delivery.",
        )
        self.assertIsNotNone(self.delivery.claim_token)
        self.assertEqual(
            OutboxMessage.objects.get(delivery=self.delivery).status,
            OutboxMessage.Status.PENDING,
        )

    def test_fresh_heartbeat_prevents_recovery_when_updated_at_is_old(self):
        token = uuid.uuid4()
        self.delivery.status = Delivery.Status.PROCESSING
        self.delivery.claim_token = token
        self.delivery.worker_heartbeat_at = timezone.now()
        self.delivery.save(
            update_fields=[
                "status",
                "claim_token",
                "worker_heartbeat_at",
            ]
        )

        Delivery.objects.filter(pk=self.delivery.pk).update(
            updated_at=timezone.now() - timedelta(minutes=10)
        )

        self.run_processor()

        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.status, Delivery.Status.PROCESSING)
        self.assertEqual(self.delivery.claim_token, token)
        self.assertEqual(OutboxMessage.objects.count(), 0)

    def test_stale_heartbeat_triggers_recovery_when_updated_at_is_recent(self):
        token = uuid.uuid4()
        self.delivery.status = Delivery.Status.PROCESSING
        self.delivery.claim_token = token
        self.delivery.worker_heartbeat_at = (
            timezone.now() - timedelta(minutes=10)
        )
        self.delivery.save(
            update_fields=[
                "status",
                "claim_token",
                "worker_heartbeat_at",
            ]
        )
        Delivery.objects.filter(pk=self.delivery.pk).update(
            updated_at=timezone.now()
        )

        self.run_processor()

        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.status, Delivery.Status.PROCESSING)
        self.assertNotEqual(self.delivery.claim_token, token)
        self.assertEqual(
            self.delivery.last_error,
            "Recovered stale processing delivery.",
        )
        self.assertIsNotNone(self.delivery.worker_heartbeat_at)
        self.assertEqual(
            OutboxMessage.objects.get(delivery=self.delivery).status,
            OutboxMessage.Status.PENDING,
        )

    def test_stale_processing_with_pending_outbox_is_not_recovered(self):
        token = uuid.uuid4()
        self.delivery.status = Delivery.Status.PROCESSING
        self.delivery.claim_token = token
        self.delivery.save(update_fields=["status", "claim_token"])

        OutboxMessage.objects.create(
            delivery=self.delivery,
            claim_token=token,
            task_payload={"delivery_id": self.delivery.pk},
            next_attempt_at=timezone.now() + timedelta(minutes=5),
        )

        Delivery.objects.filter(pk=self.delivery.pk).update(
            updated_at=timezone.now() - timedelta(minutes=10)
        )

        self.run_processor()

        self.delivery.refresh_from_db()
        self.assertEqual(
            self.delivery.status,
            Delivery.Status.PROCESSING,
        )
        self.assertEqual(self.delivery.claim_token, token)
        self.assertEqual(OutboxMessage.objects.count(), 1)

    def test_stale_delivery_at_attempt_limit_is_failed(self):
        self.delivery.status = Delivery.Status.PROCESSING
        self.delivery.attempt_count = self.delivery.max_attempts
        self.delivery.save(update_fields=["status", "attempt_count"])

        Delivery.objects.filter(pk=self.delivery.pk).update(
            updated_at=timezone.now() - timedelta(minutes=10)
        )

        self.run_processor()

        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.status, Delivery.Status.FAILED)
        self.assertIsNone(self.delivery.next_retry_at)
        self.assertEqual(OutboxMessage.objects.count(), 0)

class DeliveryResultCallbackTests(TestCase):
    def setUp(self):
        self.client = APIClient()

        event = Event.objects.create(
            event_type="order.created",
            payload={
                "order_ref": "ORD-CALLBACK-TEST",
                "amount_minor": 200000,
                "currency": "PHP",
                "status": "placed",
            },
        )

        endpoint = WebhookEndpoint.objects.create(
            slug="callback-test",
            name="Callback Test",
            url="http://127.0.0.1:8002/api/v1/webhooks/hookrelay",
            event_types=["order.created"],
            is_active=True,
        )

        self.delivery = Delivery.objects.create(
            event=event,
            endpoint=endpoint,
            status=Delivery.Status.PROCESSING,
            attempt_count=0,
            max_attempts=5,
            claim_token=uuid.uuid4(),
        )

        self.url = f"/api/deliveries/{self.delivery.pk}/result/"

    def test_heartbeat_refreshes_current_claim(self):
        before = timezone.now() - timedelta(minutes=2)
        Delivery.objects.filter(pk=self.delivery.pk).update(
            worker_heartbeat_at=before
        )

        response = self.client.post(
            f"/api/deliveries/{self.delivery.pk}/heartbeat/",
            {"claim_token": str(self.delivery.claim_token)},
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data["active"])
        self.delivery.refresh_from_db()
        self.assertGreater(self.delivery.worker_heartbeat_at, before)

    def test_heartbeat_ignores_stale_claim_token(self):
        current_token = self.delivery.claim_token
        old_heartbeat = timezone.now() - timedelta(minutes=2)
        Delivery.objects.filter(pk=self.delivery.pk).update(
            worker_heartbeat_at=old_heartbeat
        )

        response = self.client.post(
            f"/api/deliveries/{self.delivery.pk}/heartbeat/",
            {"claim_token": str(uuid.uuid4())},
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.data["active"])
        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.claim_token, current_token)
        self.assertEqual(self.delivery.worker_heartbeat_at, old_heartbeat)

    def test_duplicate_callback_does_not_create_another_attempt(self):
        attempt_id = str(uuid.uuid4())
        payload = {
            "attempt_id": attempt_id,
            "claim_token": str(self.delivery.claim_token),
            "success": True,
            "http_status_code": 201,
            "response_time_ms": 25,
            "response_body": '{"status":"processed"}',
            "error_message": "",
        }

        first_response = self.client.post(
            self.url, payload, format="json"
        )
        second_response = self.client.post(
            self.url, payload, format="json"
        )

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(
            second_response.data["message"],
            "Callback already processed.",
        )

        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.attempt_count, 1)
        self.assertEqual(
            self.delivery.status,
            Delivery.Status.DELIVERED,
        )
        self.assertEqual(
            DeliveryAttempt.objects.filter(
                delivery=self.delivery
            ).count(),
            1,
        )

    def test_stale_claim_callback_is_ignored(self):
        current_token = self.delivery.claim_token
        stale_token = uuid.uuid4()
        self.assertNotEqual(stale_token, current_token)

        response = self.client.post(
            self.url,
            {
                "attempt_id": str(uuid.uuid4()),
                "claim_token": str(stale_token),
                "success": True,
                "http_status_code": 201,
                "response_time_ms": 25,
                "response_body": "",
                "error_message": "",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.data["attempt"])
        self.assertIn("claim is no longer current", response.data["message"])

        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.status, Delivery.Status.PROCESSING)
        self.assertEqual(self.delivery.attempt_count, 0)
        self.assertEqual(self.delivery.claim_token, current_token)
        self.assertEqual(
            DeliveryAttempt.objects.filter(delivery=self.delivery).count(),
            0,
        )

    def test_late_callback_does_not_change_failed_delivery(self):
        self.delivery.status = Delivery.Status.FAILED
        self.delivery.attempt_count = 5
        self.delivery.save(
            update_fields=["status", "attempt_count"]
        )

        response = self.client.post(
            self.url,
            {
                "attempt_id": str(uuid.uuid4()),
                "claim_token": str(self.delivery.claim_token),
                "success": True,
                "http_status_code": 201,
                "response_time_ms": 25,
                "response_body": "",
                "error_message": "",
            },
            format="json",
        )

        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.data["attempt"])
        self.assertIn("already terminal", response.data["message"])

        self.delivery.refresh_from_db()
        self.assertEqual(
            self.delivery.status,
            Delivery.Status.FAILED,
        )
        self.assertEqual(self.delivery.attempt_count, 5)
        self.assertEqual(
            DeliveryAttempt.objects.filter(
                delivery=self.delivery
            ).count(),
            0,
        )

class ManualDeliveryRetryTests(TestCase):
    def setUp(self):
        self.client = APIClient()

        event = Event.objects.create(
            event_type="order.created",
            payload={"order_ref": "ORD-MANUAL-RETRY"},
        )
        endpoint = WebhookEndpoint.objects.create(
            slug="manual-retry-test",
            name="Manual Retry Test",
            url="http://127.0.0.1:8002/api/v1/webhooks/hookrelay",
            event_types=["order.created"],
            is_active=True,
        )
        self.delivery = Delivery.objects.create(
            event=event,
            endpoint=endpoint,
            status=Delivery.Status.FAILED,
            attempt_count=5,
            max_attempts=5,
            last_error="Maximum automatic attempts reached.",
        )
        self.url = f"/api/deliveries/{self.delivery.pk}/retry/"

    def test_failed_delivery_can_be_queued_for_manual_retry(self):
        response = self.client.post(self.url, {}, format="json")

        self.assertEqual(response.status_code, 202)
        self.assertEqual(
            response.data["message"],
            "Manual retry queued for publication.",
        )

        self.delivery.refresh_from_db()
        self.assertEqual(
            self.delivery.status,
            Delivery.Status.PROCESSING,
        )
        self.assertEqual(self.delivery.attempt_count, 5)
        self.assertIsNone(self.delivery.next_retry_at)
        self.assertEqual(self.delivery.last_error, "")
        self.assertIsNotNone(self.delivery.claim_token)

        outbox = OutboxMessage.objects.get(delivery=self.delivery)
        self.assertEqual(outbox.status, OutboxMessage.Status.PENDING)
        self.assertEqual(outbox.claim_token, self.delivery.claim_token)
        self.assertEqual(
            outbox.task_payload["delivery_id"],
            self.delivery.pk,
        )

    def test_non_failed_delivery_cannot_be_retried(self):
        self.delivery.status = Delivery.Status.DELIVERED
        self.delivery.save(update_fields=["status"])

        response = self.client.post(self.url, {}, format="json")

        self.assertEqual(response.status_code, 409)
        self.assertEqual(OutboxMessage.objects.count(), 0)

        self.delivery.refresh_from_db()
        self.assertEqual(
            self.delivery.status,
            Delivery.Status.DELIVERED,
        )

    def test_outbox_creation_failure_rolls_back_manual_retry(self):
        with patch(
            "apps.deliveries.views.enqueue_delivery_task",
            side_effect=RuntimeError("Database unavailable"),
        ):
            with self.assertRaises(RuntimeError):
                self.client.post(self.url, {}, format="json")

        self.delivery.refresh_from_db()
        self.assertEqual(self.delivery.status, Delivery.Status.FAILED)
        self.assertIsNone(self.delivery.claim_token)
        self.assertEqual(
            self.delivery.last_error,
            "Maximum automatic attempts reached.",
        )
        self.assertEqual(OutboxMessage.objects.count(), 0)


class RetrySchedulerCommandTests(SimpleTestCase):
    command_path = (
        "apps.deliveries.management.commands."
        "run_delivery_retry_scheduler"
    )

    @patch(f"{command_path}.time.sleep", side_effect=KeyboardInterrupt)
    @patch(f"{command_path}.call_command")
    def test_runs_retry_and_outbox_commands_and_stops_cleanly(
        self, call_command_mock, sleep_mock
    ):
        call_command(
            "run_delivery_retry_scheduler",
            interval=0.5,
            batch_size=25,
            stale_after_seconds=120,
        )

        self.assertEqual(
            call_command_mock.call_args_list,
            [
                call(
                    "process_delivery_retries",
                    batch_size=25,
                    stale_after_seconds=120,
                ),
                call("dispatch_outbox", batch_size=25),
            ],
        )
        sleep_mock.assert_called_once_with(0.5)

    @patch(f"{command_path}.time.sleep", side_effect=KeyboardInterrupt)
    @patch(f"{command_path}.call_command")
    def test_outbox_dispatch_runs_even_if_retry_processing_fails(
        self, call_command_mock, sleep_mock
    ):
        from io import StringIO

        call_command_mock.side_effect = [
            RuntimeError("Simulated scheduler failure"),
            None,
        ]
        stdout = StringIO()
        stderr = StringIO()

        call_command(
            "run_delivery_retry_scheduler",
            interval=1,
            stdout=stdout,
            stderr=stderr,
        )

        self.assertEqual(
            call_command_mock.call_args_list,
            [
                call(
                    "process_delivery_retries",
                    batch_size=100,
                    stale_after_seconds=300,
                ),
                call("dispatch_outbox", batch_size=100),
            ],
        )
        sleep_mock.assert_called_once_with(1)
        self.assertIn(
            "process_delivery_retries failed: Simulated scheduler failure",
            stderr.getvalue(),
        )
        self.assertIn("Retry scheduler stopped", stdout.getvalue())

    def test_invalid_options_are_rejected(self):
        for options, expected_message in (
            ({"interval": 0}, "--interval must be greater than zero."),
            ({"batch_size": 0}, "--batch-size must be greater than zero."),
            (
                {"stale_after_seconds": 0},
                "--stale-after-seconds must be greater than zero.",
            ),
        ):
            with self.subTest(options=options):
                with self.assertRaisesMessage(
                    Exception, expected_message
                ):
                    call_command("run_delivery_retry_scheduler", **options)
