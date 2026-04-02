from django.core.management.base import BaseCommand

from hordak.models import Account, Leg


class Command(BaseCommand):
    help = "Rebuild running total checkpoints for all accounts with legs."

    def add_arguments(self, parser):
        parser.add_argument(
            "--keep-history",
            action="store_true",
            default=False,
            help="Append new checkpoints instead of replacing existing ones.",
        )

    def handle(self, *args, **options):
        accounts = Account.objects.filter(
            pk__in=Leg.objects.order_by()
            .values_list("account_id", flat=True)
            .distinct()
        )

        for account in accounts.iterator():
            account.rebuild_running_totals(keep_history=options["keep_history"])

        self.stdout.write(
            f"Rebuilt running total checkpoints for {accounts.count()} accounts."
        )
