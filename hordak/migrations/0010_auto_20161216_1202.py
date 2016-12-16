# -*- coding: utf-8 -*-
# Generated by Django 1.10.4 on 2016-12-16 18:02
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('hordak', '0009_bank_accounts_are_asset_accounts'),
    ]

    operations = [
        migrations.AlterField(
            model_name='account',
            name='_type',
            field=models.CharField(blank=True, choices=[('AS', 'Asset'), ('LI', 'Liability'), ('IN', 'Income'), ('EX', 'Expense'), ('EQ', 'Equity'), ('TR', 'Currency Trading')], max_length=2),
        ),
        migrations.AlterField(
            model_name='account',
            name='is_bank_account',
            field=models.BooleanField(default=False, help_text='Is this a bank account. This implies we can import bank statements into it and that it only supports a single currency'),
        ),
    ]
