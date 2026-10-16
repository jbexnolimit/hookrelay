from django.conf import settings
from django.contrib import admin
from django.urls import include, path

from apps.core.views import HealthCheckView

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/health/", HealthCheckView.as_view(), name="health-check"),
    # Each app URL module registers its own prefix (webhooks/, events/, deliveries/).
    path("api/", include("apps.webhooks.urls")),
    path("api/", include("apps.events.urls")),
    path("api/", include("apps.deliveries.urls")),
    path("api/analytics/", include("apps.analytics.urls")),
]

# Simulated webhook destinations are available only in DEBUG mode.
if settings.DEBUG:
    urlpatterns += [
        path("api/dev/", include("apps.devtools.urls")),
    ]