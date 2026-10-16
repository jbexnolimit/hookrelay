from rest_framework import viewsets
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import AllowAny

from .models import WebhookEndpoint
from .serializers import WebhookEndpointSerializer


class WebhookEndpointViewSet(viewsets.ModelViewSet):
    """Local-MVP CRUD for webhook endpoint configuration."""

    serializer_class = WebhookEndpointSerializer
    permission_classes = [AllowAny]  # Add authentication before production deployment.
    queryset = WebhookEndpoint.objects.all()

    def get_queryset(self):
        queryset = super().get_queryset()

        active_param = self.request.query_params.get("is_active")
        if active_param is not None:
            normalized = active_param.lower()
            if normalized not in {"true", "false"}:
                raise ValidationError({"is_active": "Use true or false."})
            queryset = queryset.filter(is_active=(normalized == "true"))

        event_type = self.request.query_params.get("event_type")
        if event_type:
            queryset = queryset.filter(event_types__contains=[event_type])

        return queryset
