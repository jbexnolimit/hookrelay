from django.contrib import admin

from .models import RecoveryReceiverConfig


@admin.register(RecoveryReceiverConfig)
class RecoveryReceiverConfigAdmin(admin.ModelAdmin):
    list_display = (
        "key",
        "failures_before_success",
        "attempts_received",
        "updated_at",
    )
    fields = ("key", "failures_before_success", "attempts_received")
    readonly_fields = ("attempts_received",)
    actions = ("reset_attempt_counts",)

    @admin.action(description="Reset selected receiver attempt counts")
    def reset_attempt_counts(self, request, queryset):
        updated = queryset.update(attempts_received=0)
        self.message_user(
            request,
            f"Reset attempt counts for {updated} configuration(s).",
        )
