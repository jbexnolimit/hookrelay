import logging
import uuid

from django.db import transaction
from django.utils import timezone
from rest_framework import mixins, status, viewsets
from rest_framework.decorators import action
from rest_framework.exceptions import ValidationError
from rest_framework.permissions import AllowAny
from rest_framework.response import Response

from .outbox import enqueue_delivery_task

from .models import Delivery, DeliveryAttempt
from .retry import schedule_retry
from .serializers import (
    DeliveryAttemptSerializer,
    DeliveryHeartbeatInputSerializer,
    DeliveryResultInputSerializer,
    DeliverySerializer,
)

logger = logging.getLogger(__name__)

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

    @action(detail=True, methods=["post"], url_path="retry")
    def retry(self, request, pk=None):
        """Queue a failed delivery for publication through the outbox."""
        delivery = self.get_object()

        with transaction.atomic():
            delivery = (
                Delivery.objects.select_for_update()
                .select_related("event", "endpoint")
                .get(pk=delivery.pk)
            )

            if delivery.status != Delivery.Status.FAILED:
                return Response(
                    {"message": "Only failed deliveries can be retried."},
                    status=status.HTTP_409_CONFLICT,
                )

            claim_token = uuid.uuid4()
            delivery.status = Delivery.Status.PROCESSING
            delivery.claim_token = claim_token
            delivery.worker_heartbeat_at = timezone.now()
            delivery.next_retry_at = None
            delivery.last_error = ""
            delivery.save(
                update_fields=[
                    "status",
                    "claim_token",
                    "worker_heartbeat_at",
                    "next_retry_at",
                    "last_error",
                    "updated_at",
                ]
            )
            enqueue_delivery_task(delivery, claim_token)

        delivery.refresh_from_db()

        return Response(
            {
                "message": "Manual retry queued for publication.",
                "delivery": DeliverySerializer(delivery).data,
            },
            status=status.HTTP_202_ACCEPTED,
        )

    @action(detail=True, methods=["post"], url_path="heartbeat")
    def heartbeat(self, request, pk=None):
        """Refresh worker liveness only for the current processing claim."""
        input_serializer = DeliveryHeartbeatInputSerializer(data=request.data)
        input_serializer.is_valid(raise_exception=True)
        claim_token = input_serializer.validated_data["claim_token"]

        with transaction.atomic():
            delivery = Delivery.objects.select_for_update().get(
                pk=self.get_object().pk
            )

            if (
                delivery.status != Delivery.Status.PROCESSING
                or delivery.claim_token != claim_token
            ):
                return Response(
                    {
                        "message": (
                            "Delivery claim is no longer current; "
                            "heartbeat ignored."
                        ),
                        "active": False,
                    },
                    status=status.HTTP_200_OK,
                )

            delivery.worker_heartbeat_at = timezone.now()
            delivery.save(update_fields=["worker_heartbeat_at"])

        return Response(
            {"message": "Heartbeat recorded.", "active": True},
            status=status.HTTP_200_OK,
        )

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

            attempt_id = result_data["attempt_id"]

            existing_attempt = DeliveryAttempt.objects.filter(
                attempt_id=attempt_id
            ).first()

            if existing_attempt is not None:
                if existing_attempt.delivery_id != delivery.pk:
                    return Response(
                        {"message": "Attempt ID belongs to another delivery."},
                        status=status.HTTP_409_CONFLICT,
                    )

                return Response(
                    {
                        "message": "Callback already processed.",
                        "delivery": DeliverySerializer(delivery).data,
                        "attempt": DeliveryAttemptSerializer(existing_attempt).data,
                    },
                    status=status.HTTP_200_OK,
                )

            if delivery.status in (
                Delivery.Status.DELIVERED,
                Delivery.Status.FAILED,
            ):
                return Response(
                    {
                        "message": "Delivery is already terminal; callback ignored.",
                        "delivery": DeliverySerializer(delivery).data,
                        "attempt": None,
                    },
                    status=status.HTTP_200_OK,
                )

            if (
                delivery.status != Delivery.Status.PROCESSING
                or delivery.claim_token != result_data["claim_token"]
            ):
                return Response(
                    {
                        "message": (
                            "Delivery claim is no longer current; "
                            "callback ignored."
                        ),
                        "delivery": DeliverySerializer(delivery).data,
                        "attempt": None,
                    },
                    status=status.HTTP_200_OK,
                )

            now = timezone.now()
            attempt_number = delivery.attempt_count + 1
            succeeded = result_data["success"]
            error_message = result_data.get("error_message", "")

            # Save the individual HTTP delivery attempt.
            attempt = DeliveryAttempt.objects.create(
                delivery=delivery,
                attempt_id=attempt_id,
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
                delivery.delivered_at = None
                schedule_retry(
                    delivery,
                    result_data.get("http_status_code"),
                )

            delivery.claim_token = None
            delivery.worker_heartbeat_at = None
            delivery.save(
                update_fields=[
                    "attempt_count",
                    "last_attempt_at",
                    "last_http_status",
                    "last_error",
                    "status",
                    "delivered_at",
                    "next_retry_at",
                    "claim_token",
                    "worker_heartbeat_at",
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
