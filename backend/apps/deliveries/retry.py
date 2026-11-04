from datetime import timedelta

from django.utils import timezone

from .models import Delivery


RETRY_DELAYS_SECONDS = (30, 60, 120, 240)
RETRYABLE_HTTP_STATUS_CODES = {408, 429}


def is_retryable_failure(http_status_code):
    """Return whether a failed webhook request may succeed on retry."""
    if http_status_code is None:
        # Network errors and timeouts.
        return True

    return (
        http_status_code in RETRYABLE_HTTP_STATUS_CODES
        or 500 <= http_status_code <= 599
    )


def schedule_retry(delivery, http_status_code):
    """
    Schedule the next retry or mark the delivery permanently failed.

    attempt_count is the number of attempts already completed.
    """
    if (
        not is_retryable_failure(http_status_code)
        or delivery.attempt_count >= delivery.max_attempts
    ):
        delivery.status = Delivery.Status.FAILED
        delivery.next_retry_at = None
        return False

    delay_index = delivery.attempt_count - 1

    if delay_index < 0 or delay_index >= len(RETRY_DELAYS_SECONDS):
        delivery.status = Delivery.Status.FAILED
        delivery.next_retry_at = None
        return False

    delivery.status = Delivery.Status.QUEUED
    delivery.next_retry_at = timezone.now() + timedelta(
        seconds=RETRY_DELAYS_SECONDS[delay_index]
    )
    return True
