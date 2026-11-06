from django.db.models import Avg, Count
from django.db.models.functions import TruncDate
from django.utils import timezone
from datetime import datetime, time, timedelta
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

        success_rate_percent = (
            round(delivered / total_deliveries * 100, 2)
            if total_deliveries
            else 0
        )

        return Response({
            "total_events": total_events,
            "total_deliveries": total_deliveries,
            "delivery_status_counts": {
                "queued": queued,
                "processing": processing,
                "delivered": delivered,
                "failed": failed,
            },
            "success_rate_percent": success_rate_percent,
            "average_response_time": average_response_time
        })


class DeliveryActivityView(APIView):
    permission_classes = [AllowAny]

    def get(self, request):
        raw_days = request.query_params.get("days", "7")

        try:
            requested_days = int(raw_days)
        except (TypeError, ValueError):
            return Response(
                {"detail": "days must be one of: 7, 14, 30."},
                status=400,
            )

        if requested_days not in (7, 14, 30):
            return Response(
                {"detail": "days must be one of: 7, 14, 30."},
                status=400,
            )

        current_tz = timezone.get_current_timezone()
        today = timezone.localdate()
        start_date = today - timedelta(days=requested_days - 1)

        start_at = timezone.make_aware(
            datetime.combine(start_date, time.min),
            current_tz,
        )
        end_at = timezone.make_aware(
            datetime.combine(today + timedelta(days=1), time.min),
            current_tz,
        )

        rows = (
            Delivery.objects.filter(
                created_at__gte=start_at,
                created_at__lt=end_at,
            )
            .annotate(
                day=TruncDate("created_at", tzinfo=current_tz),
            )
            .values("day", "status")
            .annotate(total=Count("id"))
            .order_by("day")
        )

        daily_counts = {}

        for row in rows:
            day = row["day"].isoformat()
            daily_counts.setdefault(day, {})[row["status"]] = row["total"]

        days = []

        for offset in range(requested_days):
            day = start_date + timedelta(days=offset)
            date_key = day.isoformat()
            counts = daily_counts.get(date_key, {})

            days.append({
                "date": date_key,
                "queued": counts.get(Delivery.Status.QUEUED, 0),
                "processing": counts.get(Delivery.Status.PROCESSING, 0),
                "delivered": counts.get(Delivery.Status.DELIVERED, 0),
                "failed": counts.get(Delivery.Status.FAILED, 0),
            })

        return Response({"days": days})
