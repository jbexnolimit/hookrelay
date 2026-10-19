from rest_framework import mixins, status, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from django.db import transaction
from django.utils import timezone


from .models import Delivery, DeliveryAttempt
from .serializers import (
    DeliveryAttemptSerializer,
    DeliveryResultInputSerializer,
    DeliverySerializer,
)

class DeliveryViewSet(
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    viewsets.GenericViewSet,
):
    """Read-only delivery history with query-parameter filters."""

    serializer_class = DeliverySerializer
    permission_classes = [AllowAny]  # Add authentication before production deployment.
    queryset = Delivery.objects.select_related(
                "event",
                "endpoint",
            ).prefetch_related(
                "attempts",
        )

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

    

    @action(detail=True, methods=["post"], url_path="result")
    def result(self, request, pk=None):
        """Record a webhook delivery result reported by the Rust worker."""

        input_serializer = DeliveryResultInputSerializer(
            data=request.data
        )
        input_serializer.is_valid(raise_exception=True)
        result_data = input_serializer.validated_data

        # Return 404 if this delivery ID does not exist.
        delivery = self.get_object()

        with transaction.atomic():
            # Lock the row to prevent concurrent callbacks from using
            # the same attempt number.
            delivery = Delivery.objects.select_for_update().get(
                pk=delivery.pk
            )

            now = timezone.now()
            attempt_number = delivery.attempt_count + 1
            succeeded = result_data["success"]
            error_message = result_data.get("error_message", "")

            # Save the individual HTTP delivery attempt.
            attempt = DeliveryAttempt.objects.create(
                delivery=delivery,
                attempt_number=attempt_number,
                result=(
                    DeliveryAttempt.Result.SUCCESS
                    if succeeded
                    else DeliveryAttempt.Result.FAILURE
                ),
                http_status_code=result_data.get(
                    "http_status_code"
                ),
                response_time_ms=result_data.get(
                    "response_time_ms", 0
                ),
                response_body=result_data.get(
                    "response_body", ""
                ),
                error_message=error_message,
            )

            # Update the overall delivery record.
            delivery.attempt_count = attempt_number
            delivery.last_attempt_at = now
            delivery.last_http_status = result_data.get(
                "http_status_code"
            )
            delivery.last_error = error_message

            if succeeded:
                delivery.status = Delivery.Status.DELIVERED
                delivery.delivered_at = now
                delivery.next_retry_at = None
                delivery.last_error = ""
            else:
                delivery.status = Delivery.Status.FAILED
                delivery.delivered_at = None
                delivery.next_retry_at = None

            delivery.save(
                update_fields=[
                    "attempt_count",
                    "last_attempt_at",
                    "last_http_status",
                    "last_error",
                    "status",
                    "delivered_at",
                    "next_retry_at",
                    "updated_at",
                ]
            )

        return Response(
            {
                "message": "Delivery result recorded successfully.",
                "delivery": DeliverySerializer(delivery).data,
                "attempt": DeliveryAttemptSerializer(attempt).data,
            },
            status=status.HTTP_200_OK,
        )
