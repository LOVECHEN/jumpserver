# Generated by Django 4.1.10 on 2023-10-08 04:04

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('users', '0046_auto_20230927_1456'),
    ]

    operations = [
        migrations.AddField(
            model_name='user',
            name='date_api_key_last_used',
            field=models.DateTimeField(blank=True, null=True, verbose_name='Date api key used'),
        ),
    ]