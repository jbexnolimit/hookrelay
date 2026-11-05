from django.db import transaction
from django.utils import timezone
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import RecoveryReceiverConfig


class SuccessReceiverView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        return Response({
            "received": True,
            "receiver": "success-demo",
            "received_at": timezone.now(),
            "payload": request.data,
        }, status=200)


class FailureReceiverView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        return Response({
            "received": False,
            "receiver": "failure-demo",
            "message": "Simulated receiver failure for testing.",
        }, status=500)


class RecoveryReceiverView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        with transaction.atomic():
            config, _ = RecoveryReceiverConfig.objects.get_or_create(
                key="default",
            )

            config = RecoveryReceiverConfig.objects.select_for_update().get(
                pk=config.pk,
            )

            config.attempts_received += 1
            attempt_number = config.attempts_received
            should_fail = (
                attempt_number <= config.failures_before_success
            )

            config.save(update_fields=["attempts_received", "updated_at"])

            failures_before_success = config.failures_before_success

        if should_fail:
            return Response({
                "received": False,
                "receiver": "recovery-demo",
                "attempt": attempt_number,
                "failures_before_success": failures_before_success,
                "message": "Simulated temporary failure.",
            }, status=500)

        return Response({
            "received": True,
            "receiver": "recovery-demo",
            "attempt": attempt_number,
            "failures_before_success": failures_before_success,
            "message": "Recovery receiver succeeded.",
            "received_at": timezone.now(),
            "payload": request.data,
        }, status=200)
