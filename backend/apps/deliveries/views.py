from rest_framework import mixins, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from .models import Delivery
from .serializers import DeliveryAttemptSerializer, DeliverySerializer


class DeliveryViewSet(
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    viewsets.GenericViewSet,
):
    """Read-only delivery history with query-parameter filters."""

    serializer_class = DeliverySerializer
    permission_classes = [AllowAny]  # Add authentication before production deployment.
    queryset = Delivery.objects.select_related("event", "endpoint").all()

    def get_queryset(self):
        queryset = super().get_queryset()
        status_value = self.request.query_params.get("status")
        if status_value:
            if status_value not in Delivery.Status.values:
                raise ValidationError({
                    "status": f"Choose one of: {', '.join(Delivery.Status.values)}."
                })
            queryset = queryset.filter(status=status_value)

        for parameter, field in (("event_id", "event_id"), ("endpoint_id", "endpoint_id")):
            value = self.request.query_params.get(parameter)
            if value:
                try:
                    parsed = int(value)
                except ValueError as exc:
                    raise ValidationError({parameter: "Must be an integer."}) from exc
                queryset = queryset.filter(**{field: parsed})

        return queryset

    @action(detail=True, methods=["get"], url_path="attempts")
    def attempts(self, request, pk=None):
        delivery = self.get_object()
        queryset = delivery.attempts.all()
        return Response(DeliveryAttemptSerializer(queryset, many=True).data)
