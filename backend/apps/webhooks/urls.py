from rest_framework.routers import SimpleRouter

from .views import WebhookEndpointViewSet

router = SimpleRouter()
router.register(r"webhooks", WebhookEndpointViewSet, basename="webhook-endpoint")

urlpatterns = router.urls
