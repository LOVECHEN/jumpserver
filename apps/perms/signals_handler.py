# -*- coding: utf-8 -*-
#
from collections import defaultdict
from itertools import chain

from django.db.models.signals import m2m_changed, pre_delete
from django.dispatch import receiver
from django.db import transaction
from django.db.models import Q, F

from perms.async_tasks.mapping_node_task import submit_update_mapping_node_task
from perms.utils.user_node_tree import check_mapping_node_task
from users.models import User
from assets.models import Node, Asset
from common.utils import get_logger
from common.exceptions import M2MReverseNotAllowed
from common.const.signals import POST_ADD, POST_REMOVE, PRE_CLEAR, PRE_REMOVE, POST_CLEAR
from .models import AssetPermission, RemoteAppPermission, RebuildUserTreeTask
from .utils import update_users_tree_for_perm_change, ADD, REMOVE
from perms.exceptions import CanNotRemoveAssetPermNow


logger = get_logger(__file__)


# Todo: 检查授权规则到期，从而修改授权规则


@receiver([pre_delete], sender=AssetPermission)
def on_asset_permission_delete(instance, **kwargs):
    if not check_mapping_node_task():
        submit_update_mapping_node_task()
        raise CanNotRemoveAssetPermNow
    nodes = list(instance.nodes.all())
    assets = list(instance.assets.all())
    user_ap_query_name = AssetPermission.users.field.related_query_name()
    group_ap_query_name = AssetPermission.user_groups.field.related_query_name()
    user_ap_q = Q(**{f'{user_ap_query_name}': instance})
    group_ap_q = Q(**{f'groups__{group_ap_query_name}': instance})
    users = list(User.objects.filter(user_ap_q | group_ap_q).distinct())
    update_users_tree_for_perm_change(users, assets=assets, nodes=nodes, action=REMOVE)


def create_rebuild_user_tree_task(user_ids):
    RebuildUserTreeTask.objects.bulk_create(
        [RebuildUserTreeTask(user_id=i) for i in user_ids]
    )
    transaction.on_commit(submit_update_mapping_node_task)


def create_rebuild_user_tree_task_by_asset_perm(asset_perm: AssetPermission):
    user_ap_query_name = AssetPermission.users.field.related_query_name()
    group_ap_query_name = AssetPermission.user_groups.field.related_query_name()

    user_ap_q = Q(**{f'{user_ap_query_name}': asset_perm})
    group_ap_q = Q(**{f'groups__{group_ap_query_name}': asset_perm})
    user_ids = User.objects.filter(user_ap_q | group_ap_q).distinct().values_list('id', flat=True)
    create_rebuild_user_tree_task(user_ids)


@receiver(m2m_changed, sender=AssetPermission.nodes.through)
def on_permission_nodes_changed(instance, action, reverse, pk_set, **kwargs):
    if reverse:
        raise M2MReverseNotAllowed

    actions = (POST_REMOVE, POST_ADD, PRE_CLEAR)
    if action in actions:
        create_rebuild_user_tree_task_by_asset_perm(instance)

    if action != POST_ADD:
        return
    logger.debug("Asset permission nodes change signal received")
    nodes = kwargs['model'].objects.filter(pk__in=pk_set)
    system_users = instance.system_users.all()
    for system_user in system_users:
        system_user.nodes.add(*tuple(nodes))


@receiver(m2m_changed, sender=AssetPermission.assets.through)
def on_permission_assets_changed(instance, action, reverse, pk_set, model, **kwargs):
    if reverse:
        raise M2MReverseNotAllowed

    actions = (POST_REMOVE, POST_ADD, PRE_CLEAR)
    if action in actions:
        create_rebuild_user_tree_task_by_asset_perm(instance)

    if action != POST_ADD:
        return
    logger.debug("Asset permission assets change signal received")
    assets = model.objects.filter(pk__in=pk_set)

    # TODO 待优化
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
def on_asset_permission_users_changed(instance, action, reverse, pk_set, model, **kwargs):
    if reverse:
        raise M2MReverseNotAllowed

    actions = (POST_REMOVE, POST_ADD, PRE_CLEAR)
    if action in actions:
        create_rebuild_user_tree_task(pk_set)

    if action != POST_ADD:
        return
    logger.debug("Asset permission users change signal received")
    users = model.objects.filter(pk__in=pk_set)
    system_users = instance.system_users.all()

    # TODO 待优化
    for system_user in system_users:
        if system_user.username_same_with_user:
            system_user.users.add(*tuple(users))


@receiver(m2m_changed, sender=AssetPermission.user_groups.through)
def on_asset_permission_user_groups_changed(instance, action, pk_set, model,
                                            reverse, **kwargs):
    if reverse:
        raise M2MReverseNotAllowed

    actions = (POST_REMOVE, POST_ADD, PRE_CLEAR)
    if action in actions:
        user_ids = User.objects.filter(groups__id__in=pk_set).distinct().values_list('id', flat=True)
        create_rebuild_user_tree_task(user_ids)

    if action != POST_ADD:
        return
    logger.debug("Asset permission user groups change signal received")
    groups = model.objects.filter(pk__in=pk_set)
    system_users = instance.system_users.all()

    # TODO 待优化
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
    actions = (POST_REMOVE, POST_ADD, PRE_CLEAR)
    if action not in actions:
        return

    if reverse:
        asset_pk_set = pk_set
    else:
        asset_pk_set = [instance.id]

    user_ap_query_name = AssetPermission.users.field.related_query_name()
    group_ap_query_name = AssetPermission.user_groups.field.related_query_name()

    user_ap_q = Q(**{f'{user_ap_query_name}__assets__id__in': asset_pk_set})
    group_ap_q = Q(**{f'groups__{group_ap_query_name}__assets__id__in': asset_pk_set})

    from_user_ids = User.objects.filter(user_ap_q).values_list('id')
    from_group_ids = User.objects.filter(group_ap_q).values_list('id')
    create_rebuild_user_tree_task(chain(from_user_ids, from_group_ids))
