import time

from django.core.management import BaseCommand, call_command
from django.core.management.base import CommandError


class Command(BaseCommand):
    help = "Continuously process due webhook retries at a fixed interval."

    def add_arguments(self, parser):
        parser.add_argument(
            "--interval",
            type=float,
            default=10,
            help="Seconds to wait between retry scheduler runs (default: 10).",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Maximum number of retries to process per run.",
        )
        parser.add_argument(
            "--stale-after-seconds",
            type=int,
            default=300,
            help="Seconds before a processing delivery is considered stale.",
        )

    def handle(self, *args, **options):
        interval = options["interval"]
        batch_size = options["batch_size"]
        stale_after_seconds = options["stale_after_seconds"]

        if interval <= 0:
            raise CommandError("--interval must be greater than zero.")

        if batch_size <= 0:
            raise CommandError("--batch-size must be greater than zero.")

        if stale_after_seconds <= 0:
            raise CommandError(
                "--stale-after-seconds must be greater than zero."
            )

        self.stdout.write(
            self.style.SUCCESS(
                f"Retry scheduler started; checking every {interval:g} seconds. "
                "Press Ctrl+C to stop."
            )
        )

        try:
            while True:
                try:
                    call_command(
                        "process_delivery_retries",
                        batch_size=options["batch_size"],
                        stale_after_seconds=options["stale_after_seconds"],
                    )
                except Exception as exc:
                    self.stderr.write(
                        self.style.ERROR(
                            f"Retry scheduler run failed: {exc}"
                        )
                    )

                time.sleep(interval)
        except KeyboardInterrupt:
            self.stdout.write("Retry scheduler stopped.")
