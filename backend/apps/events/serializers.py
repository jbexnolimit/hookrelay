from rest_framework import serializers

from .models import Event


class EventSerializer(serializers.ModelSerializer):
    idempotency_key = serializers.CharField(
        max_length=255,
        required=False,
        allow_null=True,
        allow_blank=False,
    )

    class Meta:
        model = Event
        fields = ["id", "event_type", "payload", "idempotency_key", "created_at"]
        read_only_fields = ["id", "created_at"]

    def validate_event_type(self, value):
        value = value.strip()
        if not value:
            raise serializers.ValidationError("Event type cannot be empty.")
        return value

    def validate_payload(self, value):
        if not isinstance(value, dict):
            raise serializers.ValidationError("Payload must be a JSON object.")
        return value
