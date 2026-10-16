from django.db import transaction
from rest_framework import mixins, status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from apps.deliveries.models import Delivery
from apps.deliveries.serializers import DeliverySerializer
from apps.webhooks.models import WebhookEndpoint
from .models import Event
from .serializers import EventSerializer


class EventViewSet(
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    mixins.CreateModelMixin,
    viewsets.GenericViewSet,
):
    """Create events and inspect them; event records cannot be edited/deleted via API."""

    serializer_class = EventSerializer
    permission_classes = [AllowAny]  # Add authentication before production deployment.
    queryset = Event.objects.all()

    def _response_data(self, event, idempotent_replay=False):
        deliveries = Delivery.objects.filter(event=event).select_related(
            "event", "endpoint"
        )
        has_deliveries = deliveries.exists()
        if idempotent_replay:
            message = "This idempotency key was already processed; existing event returned."
        elif has_deliveries:
            message = (
                "Event saved and delivery records queued. Kafka/Rust dispatch "
                "will be connected in the next milestone."
            )
        else:
            message = "Event saved, but no active webhook endpoint subscribes to this event type."

        return {
            "message": message,
            "idempotent_replay": idempotent_replay,
            "event": EventSerializer(event).data,
            "deliveries": DeliverySerializer(deliveries, many=True).data,
        }

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        idempotency_key = serializer.validated_data.get("idempotency_key")
        if idempotency_key:
            existing = Event.objects.filter(
                idempotency_key=idempotency_key
            ).first()
            if existing:
                return Response(
                    self._response_data(existing, idempotent_replay=True),
                    status=status.HTTP_200_OK,
                )

        with transaction.atomic():
            event = serializer.save()
            endpoints = WebhookEndpoint.objects.filter(
                is_active=True,
                event_types__contains=[event.event_type],
            ).order_by("id")
            Delivery.objects.bulk_create(
                [Delivery(event=event, endpoint=endpoint) for endpoint in endpoints]
            )

        return Response(
            self._response_data(event),
            status=status.HTTP_201_CREATED,
        )

    @action(detail=True, methods=["get"], url_path="deliveries")
    def deliveries(self, request, pk=None):
        event = self.get_object()
        queryset = Delivery.objects.filter(event=event).select_related(
            "event", "endpoint"
        )
        return Response(DeliverySerializer(queryset, many=True).data)
