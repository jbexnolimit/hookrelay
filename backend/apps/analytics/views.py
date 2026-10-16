from django.db.models import Avg, Count
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.deliveries.models import Delivery, DeliveryAttempt
from apps.events.models import Event


class AnalyticsSummaryView(APIView):
    permission_classes = [AllowAny]  # Add authentication before production deployment.

    def get(self, request):
        status_rows = Delivery.objects.values("status").annotate(total=Count("id"))
        status_counts = {row["status"]: row["total"] for row in status_rows}

        total_events = Event.objects.count()
        total_deliveries = sum(status_counts.values())
        delivered = status_counts.get(Delivery.Status.DELIVERED, 0)
        failed = status_counts.get(Delivery.Status.FAILED, 0)
        queued = status_counts.get(Delivery.Status.QUEUED, 0)
        processing = status_counts.get(Delivery.Status.PROCESSING, 0)
        average_response_time = DeliveryAttempt.objects.filter(
            response_time_ms__isnull=False
        ).aggregate(value=Avg("response_time_ms"))["value"]

        return Response({
            "total_events": total_events,
            "total_deliveries": total_deliveries,
            "delivery_status_counts": {
                "queued": queued,
                "processing": processing,
                "delivered": delivered,
                "failed": failed,
            },
            "success_rate_percent": round(delivered / total_deliveries * 100, 2)
            if total_deliveries else 0,
            "average_response_time_ms": round(float(average_response_time), 2)
            if average_response_time is not None else None,
        })
