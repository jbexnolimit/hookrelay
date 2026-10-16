from django.utils import timezone
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView


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