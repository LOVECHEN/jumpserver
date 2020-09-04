from itertools import chain
from typing import List
from functools import reduce
from operator import or_
from uuid import uuid4
import threading

from django.db.models import F, Count, Q

from common.utils import get_logger
from common.utils.django import get_object_or_none
from common.const.distributed_lock_key import UPDATE_MAPPING_NODE_TASK_LOCK_KEY
from orgs.utils import tmp_to_root_org
from common.utils.timezone import dt_formater, now
from assets.models import Node, Asset
from django.db.transaction import atomic
from orgs import lock
from perms.models import UserGrantedMappingNode, RebuildUserTreeTask
from users.models import User

logger = get_logger(__name__)

ADD = 'add'
REMOVE = 'remove'


TMP_GRANTED_FIELD = '_granted'
TMP_ASSET_GRANTED_FIELD = '_asset_granted'


def obj_field_add(obj, field, value=1):
    new_value = getattr(obj, field, 0) + value
    setattr(obj, field, new_value)


def set_tmp_granted(obj):
    setattr(obj, TMP_GRANTED_FIELD, True)


def set_tmp_asset_granted(obj):
    setattr(obj, TMP_ASSET_GRANTED_FIELD, True)


VALUE_TEMPLATE = '{stage}:{rand_str}:thread:{thread_name}:{thread_id}:{now}'


def _generate_value(stage=lock.DOING):
    cur_thread = threading.current_thread()

    return VALUE_TEMPLATE.format(
        stage=stage,
        thread_name=cur_thread.name,
        thread_id=cur_thread.ident,
        now=dt_formater(now()),
        rand_str=uuid4()
    )


def run_user_mapping_node_task(user: User):
    key = UPDATE_MAPPING_NODE_TASK_LOCK_KEY.format(user_id=user.id)
    doing_value = _generate_value()
    commiting_value = _generate_value(stage=lock.COMMITING)

    try:
        locked = lock.acquire(key, doing_value, timeout=60)
        if not locked:
            logger.error(f'update_mapping_node_task_locked_failed for user: {user.id}')
            raise lock.SomeoneIsDoingThis

        with atomic(savepoint=False):
            tasks = RebuildUserTreeTask.objects.filter(user=user)
            if tasks:
                rebuild_mapping_nodes(user)
                tasks.delete()
                ok = lock.change_lock_state_to_commiting(key, doing_value, commiting_value)
                if not ok:
                    logger.error(f'update_mapping_node_task_timeout for user: {user.id}')
                    raise lock.Timeout
    finally:
        lock.release(key, commiting_value, doing_value)


@tmp_to_root_org()
def compute_tmp_mapping_node_from_perm(user: User):
    node_only_fields = ('id', 'key', 'parent_key')
    nodes = Node.objects.filter(
        Q(granted_by_permissions__users=user) |
        Q(granted_by_permissions__user_groups__users=user)
    ).distinct().only(*node_only_fields)

    asset_ids = Asset.objects.filter(
        Q(granted_by_permissions__users=user) |
        Q(granted_by_permissions__user_groups__users=user)
    ).distinct().values_list('id', flat=True)

    key2leaf_nodes_mapper = {}

    # 给授权节点设置 _granted 标识，同时去重
    for _node in nodes:
        if _node.key not in key2leaf_nodes_mapper:
            set_tmp_granted(_node)
            key2leaf_nodes_mapper[_node.key] = _node

    # 查询授权资产关联的节点设置
    granted_asset_nodes = Node.objects.filter(
        assets__id__in=asset_ids
    ).only(*node_only_fields)

    # 给资产授权关联的节点设置 _asset_granted 标识，同时去重
    for _node in granted_asset_nodes:
        if _node.key not in key2leaf_nodes_mapper:
            set_tmp_asset_granted(_node)
            key2leaf_nodes_mapper[_node.key] = _node
        else:
            set_tmp_asset_granted(key2leaf_nodes_mapper[_node.key])
    leaf_nodes = key2leaf_nodes_mapper.values()

    ancestor_keys = set()
    for _node in leaf_nodes:
        ancestor_keys.update(_node.get_ancestor_keys())

    # 从祖先节点 key 中去掉同时也是叶子节点的 key
    ancestor_keys -= key2leaf_nodes_mapper.keys()
    # 查出祖先节点
    ancestors = Node.objects.filter(key__in=ancestor_keys).only(*node_only_fields)
    return [*key2leaf_nodes_mapper.values(), *ancestors]


def create_mapping_nodes(user, nodes, clear=True):
    to_create = []
    for node in nodes:
        _granted = getattr(node, TMP_GRANTED_FIELD, False)
        _asset_granted = getattr(node, TMP_ASSET_GRANTED_FIELD, False)
        to_create.append(UserGrantedMappingNode(
            user=user,
            node=node,
            key=node.key,
            parent_key=node.parent_key,
            granted=_granted,
            asset_granted=_asset_granted
        ))

    if clear:
        UserGrantedMappingNode.objects.filter(user=user).delete()
    UserGrantedMappingNode.objects.bulk_create(to_create)


def rebuild_mapping_nodes(user):
    tmp_nodes = compute_tmp_mapping_node_from_perm(user)
    create_mapping_nodes(user, tmp_nodes)
