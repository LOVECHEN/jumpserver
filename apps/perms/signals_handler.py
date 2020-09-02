# -*- coding: utf-8 -*-
#
from collections import defaultdict
from itertools import chain

from django.db.models.signals import m2m_changed, pre_delete
from django.dispatch import receiver
from django.db import transaction
from django.db.models import Q, F

from perms.async_tasks.mapping_node_task import submit_update_mapping_node_task
from users.models import User
from assets.models import Node, Asset
from common.utils import get_logger
from common.const.signals import POST_ADD, POST_REMOVE, PRE_CLEAR
from .models import AssetPermission, RemoteAppPermission, UpdateMappingNodeTask
from .utils import update_users_tree_for_perm_change, ADD, REMOVE, on_node_asset_change


logger = get_logger(__file__)


# Todo: 检查授权规则到期，从而修改授权规则


@receiver([pre_delete], sender=AssetPermission)
def on_permission_change(instance, **kwargs):
    nodes = list(instance.nodes.all())
    assets = list(instance.assets.all())
    user_ap_query_name = AssetPermission.users.field.related_query_name()
    group_ap_query_name = AssetPermission.user_groups.field.related_query_name()
    user_ap_q = Q(**{f'{user_ap_query_name}': instance})
    group_ap_q = Q(**{f'groups__{group_ap_query_name}': instance})
    users = list(User.objects.filter(user_ap_q | group_ap_q).distinct())
    update_users_tree_for_perm_change(users, assets=assets, nodes=nodes, action=REMOVE)


@receiver(m2m_changed, sender=AssetPermission.nodes.through)
def on_permission_nodes_changed(sender, instance, action, reverse, **kwargs):
    pk_set = kwargs.get('pk_set', [])

    user_ap_query_name = AssetPermission.users.field.related_query_name()
    group_ap_query_name = AssetPermission.user_groups.field.related_query_name()

    if isinstance(instance, AssetPermission):
        user_ap_q = Q(**{f'{user_ap_query_name}': instance})
        group_ap_q = Q(**{f'groups__{group_ap_query_name}': instance})
        users = list(User.objects.filter(user_ap_q | group_ap_q).distinct())
        nodes = list(Node.objects.filter(id__in=pk_set))
    else:
        user_ap_q = Q(**{f'{user_ap_query_name}__id__in': pk_set})
        group_ap_q = Q(**{f'groups__{group_ap_query_name}__id__in': pk_set})
        users = list(User.objects.filter(user_ap_q | group_ap_q).distinct())
        nodes = [instance]

    if action == POST_ADD:
        _action = ADD
    elif action == POST_REMOVE:
        _action = REMOVE
    else:
        # Not support `clear`
        _action = None
    if _action:
        update_users_tree_for_perm_change(users, nodes=nodes, action=_action)

    if action != POST_ADD and reverse:
        return
    logger.debug("Asset permission nodes change signal received")
    nodes = kwargs['model'].objects.filter(pk__in=pk_set)
    system_users = instance.system_users.all()
    for system_user in system_users:
        system_user.nodes.add(*tuple(nodes))


@receiver(m2m_changed, sender=AssetPermission.assets.through)
def on_permission_assets_changed(sender, instance=None, action='', reverse=None, **kwargs):
    pk_set = kwargs.get('pk_set', [])

    user_ap_query_name = AssetPermission.users.field.related_query_name()
    group_ap_query_name = AssetPermission.user_groups.field.related_query_name()

    if isinstance(instance, AssetPermission):
        user_ap_q = Q(**{f'{user_ap_query_name}': instance})
        group_ap_q = Q(**{f'groups__{group_ap_query_name}': instance})
        users = list(User.objects.filter(user_ap_q | group_ap_q).distinct())
        assets = list(Asset.objects.filter(id__in=pk_set))
    else:
        user_ap_q = Q(**{f'{user_ap_query_name}__id__in': pk_set})
        group_ap_q = Q(**{f'groups__{group_ap_query_name}__id__in': pk_set})
        users = list(User.objects.filter(user_ap_q | group_ap_q).distinct())
        assets = [instance]

    if action == POST_ADD:
        _action = ADD
    elif action == POST_REMOVE:
        _action = REMOVE
    else:
        # Not support `clear`
        _action = None
    if _action:
        update_users_tree_for_perm_change(users, assets=assets, action=_action)

    if action != POST_ADD and reverse:
        return
    logger.debug("Asset permission assets change signal received")
    assets = kwargs['model'].objects.filter(pk__in=kwargs['pk_set'])
    system_users = instance.system_users.all()
    for system_user in system_users:
        system_user.assets.add(*tuple(assets))


@receiver(m2m_changed, sender=AssetPermission.system_users.through)
def on_asset_permission_system_users_changed(sender, instance=None, action='',
                                             reverse=False, **kwargs):
    if action != POST_ADD and reverse:
        return
    logger.debug("Asset permission system_users change signal received")
    system_users = kwargs['model'].objects.filter(pk__in=kwargs['pk_set'])
    assets = instance.assets.all().values_list('id', flat=True)
    nodes = instance.nodes.all().values_list('id', flat=True)
    users = instance.users.all().values_list('id', flat=True)
    groups = instance.user_groups.all().values_list('id', flat=True)
    for system_user in system_users:
        system_user.nodes.add(*tuple(nodes))
        system_user.assets.add(*tuple(assets))
        if system_user.username_same_with_user:
            system_user.groups.add(*tuple(groups))
            system_user.users.add(*tuple(users))


@receiver(m2m_changed, sender=AssetPermission.users.through)
def on_asset_permission_users_changed(sender, instance=None, action='',
                                      reverse=False, **kwargs):
    pk_set = kwargs.get('pk_set', [])

    if isinstance(instance, AssetPermission):
        nodes = list(instance.nodes.all())
        assets = list(instance.assets.all())
        users = list(User.objects.filter(id__in=pk_set).distinct())
    else:
        nodes = list(Node.objects.filter(granted_by_permissions__id__in=pk_set))
        assets = list(Asset.objects.filter(granted_by_permissions__id__in=pk_set))
        users = [instance]

    if action == POST_ADD:
        _action = ADD
    elif action == POST_REMOVE:
        _action = REMOVE
    else:
        # Not support `clear`
        _action = None
    if _action:
        update_users_tree_for_perm_change(users, nodes=nodes, assets=assets, action=_action)

    if action != POST_ADD and reverse:
        return
    logger.debug("Asset permission users change signal received")
    users = kwargs['model'].objects.filter(pk__in=kwargs['pk_set'])
    system_users = instance.system_users.all()

    for system_user in system_users:
        if system_user.username_same_with_user:
            system_user.users.add(*tuple(users))


@receiver(m2m_changed, sender=AssetPermission.user_groups.through)
def on_asset_permission_user_groups_changed(sender, instance=None, action='',
                                            reverse=False, **kwargs):
    pk_set = kwargs.get('pk_set', [])

    if isinstance(instance, AssetPermission):
        nodes = list(instance.nodes.all())
        assets = list(instance.assets.all())
        users = list(User.objects.filter(groups__id__in=pk_set).distinct())
    else:
        nodes = list(Node.objects.filter(granted_by_permissions__id__in=pk_set))
        assets = list(Asset.objects.filter(granted_by_permissions__id__in=pk_set))
        users = list(User.objects.filter(groups=instance).distinct())

    if action == POST_ADD:
        _action = ADD
    elif action == POST_REMOVE:
        _action = REMOVE
    else:
        # Not support `clear`
        _action = None
    if _action:
        update_users_tree_for_perm_change(users, nodes=nodes, assets=assets, action=_action)

    if action != POST_ADD and reverse:
        return
    logger.debug("Asset permission user groups change signal received")
    groups = kwargs['model'].objects.filter(pk__in=kwargs['pk_set'])
    system_users = instance.system_users.all()

    for system_user in system_users:
        if system_user.username_same_with_user:
            system_user.groups.add(*tuple(groups))


@receiver(m2m_changed, sender=RemoteAppPermission.system_users.through)
def on_remote_app_permission_system_users_changed(sender, instance=None,
                                                  action='', reverse=False, **kwargs):
    if action != POST_ADD or reverse:
        return
    system_users = kwargs['model'].objects.filter(pk__in=kwargs['pk_set'])
    logger.debug("Remote app permission system_users change signal received")
    assets = instance.remote_apps.all().values_list('asset__id', flat=True)
    users = instance.users.all().values_list('id', flat=True)
    groups = instance.user_groups.all().values_list('id', flat=True)
    for system_user in system_users:
        system_user.assets.add(*tuple(assets))
        if system_user.username_same_with_user:
            system_user.groups.add(*tuple(groups))
            system_user.users.add(*tuple(users))


@receiver(m2m_changed, sender=RemoteAppPermission.users.through)
def on_remoteapps_permission_users_changed(sender, instance=None, action='',
                                      reverse=False, **kwargs):
    if action != POST_ADD and reverse:
        return
    logger.debug("Asset permission users change signal received")
    users = kwargs['model'].objects.filter(pk__in=kwargs['pk_set'])
    system_users = instance.system_users.all()

    for system_user in system_users:
        if system_user.username_same_with_user:
            system_user.users.add(*tuple(users))


@receiver(m2m_changed, sender=RemoteAppPermission.user_groups.through)
def on_remoteapps_permission_user_groups_changed(sender, instance=None, action='',
                                            reverse=False, **kwargs):
    if action != POST_ADD and reverse:
        return
    logger.debug("Asset permission user groups change signal received")
    groups = kwargs['model'].objects.filter(pk__in=kwargs['pk_set'])
    system_users = instance.system_users.all()

    for system_user in system_users:
        if system_user.username_same_with_user:
            system_user.groups.add(*tuple(groups))


@receiver(m2m_changed, sender=Asset.nodes.through)
def on_node_asset_change(action, instance, reverse, pk_set, **kwargs):
    # 不允许 `pre_clear` ，因为该信号没有 `pk_set`
    # [官网](https://docs.djangoproject.com/en/3.1/ref/signals/#m2m-changed)
    refused = (PRE_CLEAR,)
    if action in refused:
        raise ValueError

    mapper = {
        POST_REMOVE: REMOVE,
        POST_ADD: ADD
    }

    if action not in mapper:
        return

    if reverse:
        asset_pk_set = pk_set
        node_pks = [str(instance.id)]
    else:
        asset_pk_set = [instance.id]
        node_pks = [str(pk) for pk in pk_set]

    user_ap_query_name = AssetPermission.users.field.related_query_name()
    group_ap_query_name = AssetPermission.user_groups.field.related_query_name()

    user_ap_q = Q(**{f'{user_ap_query_name}__assets__id__in': asset_pk_set})
    group_ap_q = Q(**{f'groups__{group_ap_query_name}__assets__id__in': asset_pk_set})

    from_user = User.objects.filter(user_ap_q).annotate(asset_pk=F(f'{user_ap_query_name}__assets__id')).values_list('id', 'asset_pk')
    from_group = User.objects.filter(group_ap_q).annotate(asset_pk=F(f'groups__{group_ap_query_name}__assets__id')).values_list('id', 'asset_pk')

    user_asset_pk_mapper = defaultdict(set)
    for user_id, asset_id in chain(from_user, from_group):
        user_asset_pk_mapper[user_id].add(asset_id)

    to_create = []
    if user_asset_pk_mapper:
        for user_id, asset_pks in user_asset_pk_mapper.items():
            asset_pks = [str(pk) for pk in asset_pks]
            to_create.append(UpdateMappingNodeTask(
                user_id=user_id,
                node_pks=node_pks,
                asset_pks=asset_pks,
                action=mapper[action],
            ))

    UpdateMappingNodeTask.objects.bulk_create(to_create)
    transaction.on_commit(submit_update_mapping_node_task)
