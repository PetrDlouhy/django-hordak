# -*- coding: utf-8 -*-
# Generated by Django 1.10.1 on 2016-09-03 11:49
from __future__ import unicode_literals

from django.db import migrations


def create_trigger(apps, schema_editor):
    if schema_editor.connection.vendor == 'postgresql':
        schema_editor.execute("""
            CREATE OR REPLACE FUNCTION check_leg()
                RETURNS trigger AS
            $$
            DECLARE
                tx_id INT;
                non_zero RECORD;
            BEGIN
                IF (TG_OP = 'DELETE') THEN
                    tx_id := OLD.transaction_id;
                ELSE
                    tx_id := NEW.transaction_id;
                END IF;
        
        
                SELECT ABS(SUM(amount)) AS total, amount_currency AS currency
                    INTO non_zero
                    FROM hordak_leg
                    WHERE transaction_id = tx_id
                    GROUP BY amount_currency
                    HAVING ABS(SUM(amount)) > 0
                    LIMIT 1;
        
                IF FOUND THEN
                    RAISE EXCEPTION 'Sum of transaction amounts in each currency must be 0. Currency %% has non-zero total %%',
                        non_zero.currency, non_zero.total;
                END IF;
        
                RETURN NEW;
            END;
            $$
            LANGUAGE plpgsql;

        """)

    elif schema_editor.connection.vendor == 'mysql':
        # we have to call this procedure in Leg.on_commit, because MySQL does not support deferred triggers
        schema_editor.execute("""
            CREATE OR REPLACE PROCEDURE check_leg(_transaction_id INT)
            BEGIN
            DECLARE transaction_sum DECIMAL(13, 2);
            DECLARE transaction_currency VARCHAR(3);
            
            SELECT ABS(SUM(amount)) AS total, amount_currency AS currency
                INTO transaction_sum, transaction_currency
                FROM hordak_leg
                WHERE transaction_id = _transaction_id
                GROUP BY amount_currency
                HAVING ABS(SUM(amount)) > 0
                LIMIT 1;
                
            IF FOUND_ROWS() > 0 THEN
                SET @msg= CONCAT('Sum of transaction amounts must be 0, got ', transaction_sum);
                SIGNAL SQLSTATE '45000' SET
                MESSAGE_TEXT = @msg;
            END IF;            

            END
        """)
    else:
        raise NotImplementedError("Database vendor %s not supported" % schema_editor.connection.vendor)



class Migration(migrations.Migration):
    dependencies = [("hordak", "0005_account_currencies")]
    atomic = False

    operations = [
        migrations.RunPython(create_trigger),
    ]
