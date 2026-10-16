from rest_framework import serializers

from .models import WebhookEndpoint


class WebhookEndpointSerializer(serializers.ModelSerializer):
    event_types = serializers.ListField(
        child=serializers.CharField(max_length=100, allow_blank=False),
        allow_empty=True,
    )

    class Meta:
        model = WebhookEndpoint
        fields = [
            "id",
            "slug",
            "name",
            "url",
            "event_types",
            "is_active",
            "created_at",
            "updated_at",
        ]
        read_only_fields = ["id", "created_at", "updated_at"]

    def validate_event_types(self, value):
        if len(value) != len(set(value)):
            raise serializers.ValidationError(
                "Event types must not contain duplicates."
            )
        return value
