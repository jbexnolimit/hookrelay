from django.urls import include, path

urlpatterns = [
    path("api/dev/", include("apps.devtools.urls")),
]
