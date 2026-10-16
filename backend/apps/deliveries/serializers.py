from rest_framework import serializers

from .models import Delivery, DeliveryAttempt


class DeliveryAttemptSerializer(serializers.ModelSerializer):
    class Meta:
        model = DeliveryAttempt
        fields = [
            "id",
            "delivery",
            "attempt_number",
            "result",
            "http_status_code",
            "response_time_ms",
            "response_body",
            "error_message",
            "attempted_at",
        ]
        read_only_fields = fields


class DeliverySerializer(serializers.ModelSerializer):
    event_type = serializers.CharField(source="event.event_type", read_only=True)
    endpoint_name = serializers.CharField(source="endpoint.name", read_only=True)
    endpoint_slug = serializers.CharField(source="endpoint.slug", read_only=True)

    class Meta:
        model = Delivery
        fields = [
            "id",
            "event",
            "event_type",
            "endpoint",
            "endpoint_name",
            "endpoint_slug",
            "status",
            "attempt_count",
            "max_attempts",
            "next_retry_at",
            "last_attempt_at",
            "delivered_at",
            "last_http_status",
            "last_error",
            "created_at",
            "updated_at",
        ]
        read_only_fields = fields
