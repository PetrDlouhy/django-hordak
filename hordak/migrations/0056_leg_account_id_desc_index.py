from django.db import migrations, models

from hordak.utilities.migrations import select_database_type

INDEX = models.Index(fields=["account", "-id"], name="hordak_leg_acc_id_desc_idx")


def _add_index_concurrently():
    # Imported lazily: django.contrib.postgres needs a PostgreSQL driver, which
    # a MySQL-only installation does not have.
    from django.contrib.postgres.operations import AddIndexConcurrently

    return AddIndexConcurrently(model_name="leg", index=INDEX)


def _add_index():
    return migrations.AddIndex(model_name="leg", index=INDEX)


class Migration(migrations.Migration):
    """The (account, -id) index behind checkpoint delta reads.

    Separate from 0055 and non-atomic because a plain CREATE INDEX blocks every
    leg insert, update and delete while it builds -- minutes on a large ledger --
    and CONCURRENTLY cannot run inside a transaction. MySQL's ADD INDEX is
    online by default.
    """

    atomic = False

    dependencies = [
        ("hordak", "0055_running_total_checkpoints"),
    ]

    operations = [
        select_database_type(postgresql=_add_index_concurrently, mysql=_add_index)()
    ]
