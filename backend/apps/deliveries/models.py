
from django.db import models
import uuid


class Delivery(models.Model):
    class Status(models.TextChoices):
        QUEUED = "queued", "Queued"
        PROCESSING = "processing", "Processing"
        DELIVERED = "delivered", "Delivered"
        FAILED = "failed", "Failed"

    event = models.ForeignKey(
        "events.Event",
        on_delete=models.CASCADE,
        related_name="deliveries",
    )
    endpoint = models.ForeignKey(
        "webhooks.WebhookEndpoint",
        on_delete=models.PROTECT,
        related_name="deliveries",
    )

    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.QUEUED,
        db_index=True,
    )

    attempt_count = models.PositiveIntegerField(default=0)
    max_attempts = models.PositiveIntegerField(default=5)

    next_retry_at = models.DateTimeField(null=True, blank=True)
    claim_token = models.UUIDField(
        null=True,
        blank=True,
        editable=False,
    )
    worker_heartbeat_at = models.DateTimeField(
        null=True,
        blank=True,
        editable=False,
    )
    last_attempt_at = models.DateTimeField(null=True, blank=True)
    delivered_at = models.DateTimeField(null=True, blank=True)

    last_http_status = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
    )
    last_error = models.TextField(blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        constraints = [
            models.UniqueConstraint(
                fields=["event", "endpoint"],
                name="unique_event_endpoint_delivery",
            ),
        ]
        indexes = [
            models.Index(
                fields=["status", "next_retry_at"],
                name="delivery_status_retry_idx",
            ),
            models.Index(
                fields=["created_at"],
                name="delivery_created_idx",
            ),
        ]

    def __str__(self):
        return (
            f"Delivery #{self.pk}: "
            f"{self.event.event_type} -> {self.endpoint.name}"
        )


class DeliveryAttempt(models.Model):
    class Result(models.TextChoices):
        SUCCESS = "success", "Success"
        FAILURE = "failure", "Failure"

    delivery = models.ForeignKey(
        Delivery,
        on_delete=models.CASCADE,
        related_name="attempts",
    )

    attempt_number = models.PositiveIntegerField()
    result = models.CharField(
        max_length=10,
        choices=Result.choices,
    )

    http_status_code = models.PositiveSmallIntegerField(
        null=True,
        blank=True,
    )
    response_time_ms = models.PositiveIntegerField(
        null=True,
        blank=True,
    )
    response_body = models.TextField(blank=True, default="")
    error_message = models.TextField(blank=True, default="")

    attempted_at = models.DateTimeField(auto_now_add=True)
    attempt_id = models.UUIDField(
    null=True,
    blank=True,
    unique=True,
    editable=False,
    )

    class Meta:
        ordering = ["attempt_number"]
        constraints = [
            models.UniqueConstraint(
                fields=["delivery", "attempt_number"],
                name="unique_delivery_attempt_no",
            ),
        ]

    def __str__(self):
        return (
            f"Attempt {self.attempt_number} "
            f"for Delivery #{self.delivery_id}"
        )


class OutboxMessage(models.Model):
    """Durable record of a delivery task awaiting Kafka publication."""

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        PUBLISHED = "published", "Published"
        SUPERSEDED = "superseded", "Superseded"

    delivery = models.ForeignKey(
        Delivery,
        on_delete=models.CASCADE,
        related_name="outbox_messages",
    )
    claim_token = models.UUIDField(editable=False)
    task_payload = models.JSONField()
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    publish_attempts = models.PositiveIntegerField(default=0)
    next_attempt_at = models.DateTimeField()
    last_error = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    published_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["created_at", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["delivery", "claim_token"],
                name="unique_outbox_delivery_claim",
            ),
        ]
        indexes = [
            models.Index(
                fields=["status", "next_attempt_at"],
                name="outbox_status_next_idx",
            ),
        ]

    def __str__(self):
        return (
            f"Outbox #{self.pk}: delivery #{self.delivery_id} "
            f"({self.status})"
        )
