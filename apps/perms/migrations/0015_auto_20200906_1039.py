# Generated by Django 2.2.13 on 2020-09-06 02:39

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('perms', '0014_build_users_perm_tree'),
    ]

    operations = [
        migrations.RenameField(
            model_name='usergrantedmappingnode',
            old_name='asset_amount',
            new_name='assets_amount',
        ),
    ]
