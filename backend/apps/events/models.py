
from django.db import models


class Event(models.Model):
    event_type = models.CharField(max_length=100, db_index=True)
    payload = models.JSONField(default=dict)

    # Optional key supplied by the client to identify a repeated request.
    idempotency_key = models.CharField(
        max_length=255,
        unique=True,
        null=True,
        blank=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(
                fields=["event_type", "created_at"],
                name="event_type_created_idx",
            ),
        ]

    def __str__(self):
        return f"{self.event_type} #{self.pk}"
