# Generated by Django 2.2.13 on 2020-08-31 09:10

import common.fields.model
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('perms', '0019_toupdatenode'),
    ]

    operations = [
        migrations.AlterField(
            model_name='toupdatenode',
            name='asset_pks',
            field=common.fields.model.JsonListTextField(),
        ),
        migrations.AlterField(
            model_name='toupdatenode',
            name='node_pks',
            field=common.fields.model.JsonListTextField(),
        ),
    ]