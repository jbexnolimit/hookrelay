from django.db import models

# Create your models here.
class WebhookEndpoint(models.Model):
    slug = models.SlugField(max_length=80, unique=True)
    name = models.CharField(max_length=120)
    url = models.URLField(max_length=1000)
    event_types = models.JSONField(default=list)
    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["name"]
        indexes = [
            models.Index(
                fields=["is_active", "name"],
                name="webhook_active_name_idx",
            ),
        ]

    def __str__(self):
        return self.name