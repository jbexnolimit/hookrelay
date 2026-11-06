"""Helpers for recording durable Kafka delivery-task intent."""

from django.utils import timezone

from apps.deliveries.models import Delivery, OutboxMessage


def build_delivery_task_payload(delivery, claim_token):
    """Build a stable task snapshot for a particular delivery claim."""
    return {
        "delivery_id": delivery.pk,
        "claim_token": str(claim_token),
        "event_id": delivery.event_id,
        "event_type": delivery.event.event_type,
        "payload": delivery.event.payload,
        "webhook_url": delivery.endpoint.url,
    }


def enqueue_delivery_task(delivery, claim_token=None):
    """Persist a delivery task. Call inside the caller's DB transaction."""
    token = claim_token or delivery.claim_token

    if token is None:
        raise ValueError("A delivery claim token is required.")

    if (
        delivery.status != Delivery.Status.PROCESSING
        or delivery.claim_token != token
    ):
        raise ValueError(
            "The delivery must be processing under the supplied claim token."
        )

    return OutboxMessage.objects.create(
        delivery=delivery,
        claim_token=token,
        task_payload=build_delivery_task_payload(delivery, token),
        next_attempt_at=timezone.now(),
    )
