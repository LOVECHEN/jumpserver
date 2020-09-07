# Generated by Django 2.2.13 on 2020-08-21 08:20

from django.db import migrations
from django.db.models import Q


def fill_node_value(apps, schema_editor):
    Node = apps.get_model('assets', 'Node')
    Asset = apps.get_model('assets', 'Asset')
    for node in Node.objects.all():
        assets_amount = Asset.objects.filter(
            Q(nodes__key__istartswith=f'{node.key}:') | Q(nodes=node)
        ).distinct().count()
        key = node.key
        try:
            parent_key = key[:key.rindex(':')]
        except ValueError:
            parent_key = ''
        node.assets_amount = assets_amount
        node.parent_key = parent_key
        node.save()
        print(f'Fill {node} finished')


class Migration(migrations.Migration):

    dependencies = [
        ('assets', '0056_auto_20200904_1751'),
    ]

    operations = [
        migrations.RunPython(fill_node_value)
    ]