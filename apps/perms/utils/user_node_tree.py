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
from perms.models import MappingNode, UpdateMappingNodeTask
from users.models import User

logger = get_logger(__name__)

ADD = 'add'
REMOVE = 'remove'


TMP_GRANTED_FIELD = '_granted'
TMP_ASSET_GRANTED_REF_COUNT_FIELD = '_asset_granted_ref_count'
TMP_GRANTED_REF_COUNT_FIELD = '_granted_ref_count'
TMP_NODE_GRANTED_REF_COUNT_FIELD = '_node_granted_ref_count'


def print_attrs(mapping_node, node):
    tmp_fields = (
        TMP_ASSET_GRANTED_REF_COUNT_FIELD,
        TMP_GRANTED_REF_COUNT_FIELD,
        TMP_GRANTED_FIELD
    )
    fields = (f[1:] for f in tmp_fields)
    values1 = [getattr(mapping_node, field, 0) for field in fields]
    values2 = [getattr(node, field, 0) for field in tmp_fields]
    print(f'''compare values:
    valuse1 {values1}
    values2 {values2}
    ''')


def is_equal(mapping_node, node):
    tmp_fields = (
        TMP_ASSET_GRANTED_REF_COUNT_FIELD,
        TMP_GRANTED_REF_COUNT_FIELD,
        TMP_GRANTED_FIELD
    )
    fields = (f[1:] for f in tmp_fields)

    values1 = [getattr(node, field, 0) for field in tmp_fields]
    values2 = [getattr(mapping_node, field, 0) for field in fields]

    return all(v1 == v2 for v1, v2 in zip(values1, values2))


def obj_field_add(obj, field, value=1):
    new_value = getattr(obj, field, 0) + value
    setattr(obj, field, new_value)


def add_tmp_attrs(obj1, obj2, with_granted=False):
    """
    obj1 <- obj2
    """
    _asset_granted_ref_count = getattr(obj2, TMP_ASSET_GRANTED_REF_COUNT_FIELD, 0)
    _granted_ref_count = getattr(obj2, TMP_GRANTED_REF_COUNT_FIELD, 0)

    inc_tmp_granted_ref_count(obj1, _granted_ref_count)
    inc_tmp_asset_granted_ref_count(obj1, _asset_granted_ref_count)

    if with_granted:
        _granted = getattr(obj2, TMP_GRANTED_FIELD, False)
        if _granted:
            set_tmp_granted(obj1)


def set_tmp_granted(obj):
    _granted = getattr(obj, TMP_GRANTED_FIELD, False)
    if _granted:
        raise ValueError(f'{obj} repeat authorization')
    setattr(obj, TMP_GRANTED_FIELD, True)


def inc_tmp_granted_ref_count(obj, value=1):
    obj_field_add(obj, TMP_GRANTED_REF_COUNT_FIELD, value)


def inc_tmp_asset_granted_ref_count(obj, value=1):
    obj_field_add(obj, TMP_ASSET_GRANTED_REF_COUNT_FIELD, value)


def update_mapping_nodes(mapping_node_keys, user: User, nodes: List[Node], action: str):
    """
    给定一组 node ，更新或者创建对应的 MappingNode。更新的值包括
        - granted
        - granted_ref_count
        - asset_granted_ref_count

    本函数会用到对象上的一些临时变量：
        `_granted_ref_count` 授权计数，等于节点或者资产授权数的总和
        `_granted` 该节点是否直接授权
        `_asset_granted_ref_count` 资产授权计数
    """
    to_create = {}
    to_update = []
    mapping_nodes = MappingNode.objects.filter(key__in=mapping_node_keys, user=user)
    key2mapping_node_map = {mapping_node.key: mapping_node for mapping_node in mapping_nodes}
    for node in nodes:
        _granted = getattr(node, TMP_GRANTED_FIELD, False)
        _asset_granted_ref_count = getattr(node, TMP_ASSET_GRANTED_REF_COUNT_FIELD, 0)
        _granted_ref_count = getattr(node, TMP_GRANTED_REF_COUNT_FIELD, 0)

        if node.key in key2mapping_node_map:
            # 已存在的映射节点
            mapping_node = key2mapping_node_map[node.key]
            if action == ADD:
                if _granted:
                    if mapping_node.granted:
                        # 相同节点不能授权两次
                        raise ValueError('')
                    mapping_node.granted = True

                inc_tmp_asset_granted_ref_count(mapping_node, _asset_granted_ref_count)
                inc_tmp_granted_ref_count(mapping_node, _granted_ref_count)
            elif action == REMOVE:
                if _granted:
                    if not mapping_node.granted:
                        # 数据有问题
                        raise ValueError('')
                    mapping_node.granted = False
                inc_tmp_asset_granted_ref_count(mapping_node, -_asset_granted_ref_count)
                inc_tmp_granted_ref_count(mapping_node, -_granted_ref_count)

            to_update.append(mapping_node)
        else:
            # 不存在的映射节点，需要创建
            if action == REMOVE:
                # 数据有问题
                raise ValueError('')
            if node.key not in to_create:
                mapping_node = MappingNode(
                    key=node.key,
                    user=user,
                    granted=_granted,
                    granted_ref_count=_granted_ref_count,
                    asset_granted_ref_count=_asset_granted_ref_count,
                    parent_key=node.parent_key,
                    node=node,
                )
                to_create[node.key] = mapping_node
            else:
                mapping_node = to_create[node.key]
                mapping_node.granted_ref_count += _granted_ref_count
                mapping_node.asset_granted_ref_count += _asset_granted_ref_count
                if _granted:
                    if mapping_node.granted:
                        raise ValueError()
                    mapping_node.granted = True

    for n in to_update:
        n.granted_ref_count = F('granted_ref_count') + getattr(n, TMP_GRANTED_REF_COUNT_FIELD, 0)
        n.asset_granted_ref_count = F('asset_granted_ref_count') + getattr(n, TMP_ASSET_GRANTED_REF_COUNT_FIELD, 0)
    MappingNode.objects.bulk_update(to_update, ('granted', 'granted_ref_count', 'asset_granted_ref_count'))
    MappingNode.objects.bulk_create(to_create.values())


def update_ancestor_node(node2ancestor_keys_map:dict, key2ancestor_node_map:dict):
    for node, keys in node2ancestor_keys_map.items():
        for key in keys:
            ancestor = key2ancestor_node_map[key]  # TODO 404
            # 只更新 TMP_GRANTED_REF_COUNT_FIELD 字段，因为祖先的 TMP_ASSET_GRANTED_REF_COUNT_FIELD 数据不被影响
            inc_tmp_granted_ref_count(ancestor, getattr(node, TMP_GRANTED_REF_COUNT_FIELD, 0))


def update_users_tree_for_perm_change(users,
                                      nodes=(),
                                      assets=(),
                                      action=ADD):
    """
    `_granted_ref_count` 授权计数，等于节点或者资产授权数的总和
    `_granted` 该节点是否直接授权
    `_asset_granted_ref_count` 资产授权计数
    """

    # 查询授权`Asset`关联的 `Node`
    asset_granted_nodes_qs = Node.objects.filter(
        assets__in=assets
    ).annotate(
        _granted_ref_count=Count('assets', distinct=True),
        _asset_granted_ref_count=Count('assets', distinct=True),
    ).distinct()

    # 由于资产授权而产生的节点，该节点的属性值:
    # `_granted_ref_count`: 授权资产数量
    # `_asset_granted_ref_count`: 授权资产数量
    # `_granted`: `False`
    asset_granted_nodes = []
    for n in asset_granted_nodes_qs:
        n._granted = False
        asset_granted_nodes.append(n)

    # 直接授权的 `Node`，该节点属性值：
    # `_granted_ref_count`: 1
    # `_granted`: `True`
    for n in nodes:
        inc_tmp_granted_ref_count(n)
        n._granted = True

    # 资产授权节点与直接授权节点总共的祖先`key`，因为两者可能会重叠，所以字典的键复杂
    node2ancestor_keys_map = {n: n.get_ancestor_keys() for n in nodes}
    asset_granted_nodes2ancestor_keys_map = {n: n.get_ancestor_keys() for n in asset_granted_nodes}

    ancestor_keys_groups = [*node2ancestor_keys_map.values(), *asset_granted_nodes2ancestor_keys_map.values()]

    # 查询出要用的祖先节点
    key2ancestor_node_map = {node.key: node for node in
                             Node.objects.filter(key__in=set(chain(*ancestor_keys_groups)))}

    update_ancestor_node(node2ancestor_keys_map, key2ancestor_node_map)
    update_ancestor_node(asset_granted_nodes2ancestor_keys_map, key2ancestor_node_map)

    # 整合所有的 key
    keys = reduce(or_, (
        key2ancestor_node_map.keys(),
        {n.key for n in nodes},
        {n.key for n in asset_granted_nodes}
    ))

    # 授权节点，资产授权节点，祖先节点
    all_nodes = [*nodes, *asset_granted_nodes, *key2ancestor_node_map.values()]

    for user in users:
        # 每个用户单独处理自己的树
        update_mapping_nodes(keys, user, all_nodes, action)


def on_node_asset_change(user, nodes: List[Node], assets_amount, action):
    """
    当用户被授权的资产被移动后，更新自己的授权树
    """

    for node in nodes:
        setattr(node, TMP_ASSET_GRANTED_REF_COUNT_FIELD, assets_amount)
        setattr(node, TMP_GRANTED_REF_COUNT_FIELD, assets_amount)

    node2ancestor_keys_map = {n: n.get_ancestor_keys() for n in nodes}

    # 查询出要用的祖先节点
    key2ancestor_node_map = {node.key: node for node in
                             Node.objects.filter(key__in=set(chain(*node2ancestor_keys_map.values())))}

    update_ancestor_node(node2ancestor_keys_map, key2ancestor_node_map)

    # 整合所有的 key
    keys = reduce(or_, (
        key2ancestor_node_map.keys(),
        {n.key for n in nodes},
    ))

    # 资产授权节点，祖先节点
    all_nodes = [*nodes, *key2ancestor_node_map.values()]
    update_mapping_nodes(keys, user, all_nodes, action)


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
            raise lock.SomeoneIsDoingThis

        with atomic(savepoint=False):
            tasks = UpdateMappingNodeTask.objects.filter(user=user).order_by('date_created')
            if tasks:
                to_delete = []
                for task in tasks:
                    nodes = Node.objects.filter(id__in=task.node_pks)
                    on_node_asset_change(user, nodes, len(task.asset_pks), task.action)
                    to_delete.append(task.id)
                UpdateMappingNodeTask.objects.filter(id__in=to_delete).delete()

                ok = lock.change_lock_state_to_commiting(key, doing_value, commiting_value)
                if not ok:
                    logger.error(f'update_mapping_node_task_timeout for user: {user.id}')
                    raise lock.Timeout
    finally:
        lock.release(key, commiting_value, doing_value)


def check_user_mapping_node_task(user: User):
    if UpdateMappingNodeTask.objects.filter(user=user).exists():
        run_user_mapping_node_task(user)


def check_mapping_node_task():
    if UpdateMappingNodeTask.objects.exists():
        return False
    return True


@tmp_to_root_org()
def compute_tmp_mapping_node_from_perm(user: User):
    print(f'checking............. {user}')
    errors = []
    nodes = Node.objects.filter(
        Q(granted_by_permissions__users=user) | 
        Q(granted_by_permissions__user_groups__users=user)
    ).distinct()

    assets = Asset.objects.filter(
        Q(granted_by_permissions__users=user) |
        Q(granted_by_permissions__user_groups__users=user)
    ).distinct()

    leaf_nodes = []

    for node in nodes:
        inc_tmp_granted_ref_count(node)
        set_tmp_granted(node)
        leaf_nodes.append(node)

    for asset in assets:
        _nodes = asset.nodes.all()
        for node in _nodes:
            inc_tmp_asset_granted_ref_count(node)
            inc_tmp_granted_ref_count(node)
            leaf_nodes.append(node)

    all_ancestor_node_keys = set()
    for node in leaf_nodes:
        all_ancestor_node_keys.update(node.get_ancestor_keys())

    ancestor_nodes = list(Node.objects.filter(key__in=all_ancestor_node_keys))
    key2ancestor_nodes_mapper = {node.key: node for node in ancestor_nodes}

    for node in leaf_nodes:
        keys = node.get_ancestor_keys()
        _granted_ref_count = getattr(node, TMP_GRANTED_REF_COUNT_FIELD, 0)
        for key in keys:
            ancestor_node = key2ancestor_nodes_mapper[key]
            inc_tmp_granted_ref_count(
                ancestor_node,
                _granted_ref_count
            )

    key2nodes_mapper = {}
    for node in chain(leaf_nodes, ancestor_nodes):
        if node.key in key2nodes_mapper:
            dst_node = key2nodes_mapper[node.key]
            add_tmp_attrs(dst_node, node, with_granted=True)
        else:
            key2nodes_mapper[node.key] = node
    return key2nodes_mapper


def check_mapping_nodes(key2nodes_mapper):
    errors = []
    for key, node in key2nodes_mapper.items():
        mapping_node = get_object_or_none(MappingNode, key=key)
        if mapping_node is None:
            print_attrs(mapping_node, node)
            errors.append((mapping_node, node, 'not found'))
        elif not is_equal(mapping_node, node):
            print_attrs(mapping_node, node)
            errors.append((mapping_node, node, 'not equal'))
    return errors


def check_all_users():
    users = User.objects.all()
    for user in users:
        key2nodes_mapper = compute_tmp_mapping_node_from_perm(user)
        check_mapping_nodes(key2nodes_mapper)


@tmp_to_root_org()
def migrate_perms2mapping_node():
    users = User.objects.all()
    for user in users:
        key2nodes_mapper = compute_tmp_mapping_node_from_perm(user)
        update_mapping_nodes(key2nodes_mapper.keys(), user, key2nodes_mapper.values(), ADD)
