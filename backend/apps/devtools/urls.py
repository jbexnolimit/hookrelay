from django.urls import path

from .views import FailureReceiverView, SuccessReceiverView


urlpatterns = [
    path(
        "receiver/success/",
        SuccessReceiverView.as_view(),
        name="dev-receiver-success",
    ),
    path(
        "receiver/fail/",
        FailureReceiverView.as_view(),
        name="dev-receiver-fail",
    ),
]