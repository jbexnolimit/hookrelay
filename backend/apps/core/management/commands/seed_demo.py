import os

from django.core.management.base import BaseCommand

from apps.webhooks.models import WebhookEndpoint


class Command(BaseCommand):
    help = "Create or update HookRelay demo webhook endpoints."

    def handle(self, *args, **options):
        base_url = os.getenv("HOOKRELAY_LOCAL_BASE_URL", "http://127.0.0.1:8001").rstrip("/")
        endpoints = [
            {
                "slug": "inventory-demo",
                "name": "Inventory Demo Receiver",
                "url": f"{base_url}/api/dev/receiver/success/",
                "event_types": ["order.created"],
                "is_active": True,
            },
            {
                "slug": "failure-demo",
                "name": "Failure Demo Receiver",
                "url": f"{base_url}/api/dev/receiver/fail/",
                "event_types": ["payment.failed"],
                "is_active": True,
            },
        ]

        for endpoint_data in endpoints:
            slug = endpoint_data["slug"]
            _, created = WebhookEndpoint.objects.update_or_create(
                slug=slug,
                defaults={key: value for key, value in endpoint_data.items() if key != "slug"},
            )
            action = "Created" if created else "Updated"
            self.stdout.write(f"{action}: {endpoint_data['name']}")

        self.stdout.write(self.style.SUCCESS("HookRelay demo endpoints are ready."))
