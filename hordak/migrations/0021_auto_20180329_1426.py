# -*- coding: utf-8 -*-
# Generated by Django 1.11.11 on 2018-03-29 11:26
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [("hordak", "0020_auto_20171205_1424")]

    operations = [
        migrations.AlterField(
            model_name="statementline",
            name="transaction",
            field=models.ForeignKey(
                blank=True,
                default=None,
                help_text="Reconcile this statement line to this transaction",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                to="hordak.Transaction",
            ),
        ),
        migrations.AlterField(
            model_name="transactioncsvimportcolumn",
            name="to_field",
            field=models.CharField(
                blank=True,
                choices=[
                    (None, "-- Do not import --"),
                    ("date", "Date"),
                    ("amount", "Amount"),
                    ("amount_out", "Amount (money out only)"),
                    ("amount_in", "Amount (money in only)"),
                    ("description", "Description / Notes"),
                ],
                default=None,
                max_length=20,
                null=True,
                verbose_name="Is",
            ),
        ),
    ]
