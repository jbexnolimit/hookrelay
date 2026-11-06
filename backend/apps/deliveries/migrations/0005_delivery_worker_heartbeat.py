from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("deliveries", "0004_outboxmessage"),
    ]

    operations = [
        migrations.AddField(
            model_name="delivery",
            name="worker_heartbeat_at",
            field=models.DateTimeField(
                blank=True,
                editable=False,
                null=True,
            ),
        ),
    ]
