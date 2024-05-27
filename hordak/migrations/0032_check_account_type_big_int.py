# Generated by Django 4.0.7 on 2022-09-18 10:33

from django.db import migrations


def create_trigger(apps, schema_editor):
    if schema_editor.connection.vendor == "postgresql":
        schema_editor.execute(
            """
            CREATE OR REPLACE FUNCTION check_account_type()
                RETURNS TRIGGER AS
            $$
            BEGIN
                IF NEW.parent_id::INT::BOOL THEN
                    NEW.type = (SELECT type FROM hordak_account WHERE id = NEW.parent_id);
                END IF;
                RETURN NEW;
            END;
            $$
            LANGUAGE plpgsql;
        """
        )

    elif schema_editor.connection.vendor == "mysql":
        # we have to call this procedure in Leg.on_commit, because MySQL does not support deferred triggers
        schema_editor.execute(
            """
            CREATE OR REPLACE TRIGGER check_account_type_on_insert
            BEFORE INSERT ON hordak_account
            FOR EACH ROW
            BEGIN
                IF NEW.parent_id IS NOT NULL THEN
                    SET NEW.type = (SELECT type FROM hordak_account WHERE id = NEW.parent_id);
                END IF;
            END;
        """
        )
        schema_editor.execute(
            """
            CREATE OR REPLACE TRIGGER check_account_type_on_update
            BEFORE UPDATE ON hordak_account
            FOR EACH ROW
            BEGIN
                IF NEW.parent_id IS NOT NULL THEN
                    SET NEW.type = (SELECT type FROM hordak_account WHERE id = NEW.parent_id);
                END IF;
            END;
        """
        )
    else:
        raise NotImplementedError(
            "Database vendor %s not supported" % schema_editor.connection.vendor
        )


def drop_trigger(apps, schema_editor):
    if schema_editor.connection.vendor == "postgresql":
        schema_editor.execute("DROP FUNCTION check_account_type() CASCADE")
        # Recreate check_account_type as it was in migration 0016
        schema_editor.execute(
            """
            CREATE OR REPLACE FUNCTION check_account_type()
                RETURNS TRIGGER AS
            $$
            BEGIN
                IF NEW.parent_id::BOOL THEN
                    NEW.type = (SELECT type FROM hordak_account WHERE id = NEW.parent_id);
                END IF;
                RETURN NEW;
            END;
            $$
            LANGUAGE plpgsql;
        """
        )
        schema_editor.execute(
            """
            CREATE TRIGGER check_account_type_trigger
            BEFORE INSERT OR UPDATE ON hordak_account
            FOR EACH ROW
            WHEN (pg_trigger_depth() = 0)
            EXECUTE PROCEDURE check_account_type();
        """
        )
    elif schema_editor.connection.vendor == "mysql":
        schema_editor.execute("DROP TRIGGER check_account_type_on_insert")
        schema_editor.execute("DROP TRIGGER check_account_type_on_update")
    else:
        raise NotImplementedError(
            "Database vendor %s not supported" % schema_editor.connection.vendor
        )


class Migration(migrations.Migration):
    dependencies = (("hordak", "0031_alter_account_currencies"),)
    atomic = False

    operations = [
        migrations.RunPython(create_trigger, reverse_code=drop_trigger),
    ]
