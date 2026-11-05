from django.db import models


class RecoveryReceiverConfig(models.Model):
    key = models.CharField(
        max_length=50,
        unique=True,
        default="default",
    )
    failures_before_success = models.PositiveIntegerField(default=2)
    attempts_received = models.PositiveIntegerField(default=0)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Recovery Receiver Configuration"
        verbose_name_plural = "Recovery Receiver Configuration"

    def __str__(self):
        return (
            f"{self.key}: "
            f"{self.failures_before_success} failures before success"
        )
