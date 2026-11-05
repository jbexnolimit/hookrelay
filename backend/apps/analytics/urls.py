from django.urls import path

from .views import AnalyticsSummaryView, DeliveryActivityView

urlpatterns = [
    path("summary/", AnalyticsSummaryView.as_view(), name="analytics-summary"),
    path("activity/", DeliveryActivityView.as_view(), name="analytics-activity"),
]