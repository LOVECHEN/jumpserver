from itertools import chain
from typing import List
from functools import reduce
from operator import or_

from django.db.models import F, Count

from assets.models import Node
from perms.models import MappingNode


ADD = 'add'
REMOVE = 'remove'


TMP_GRANTED_FIELD = '_granted'
TMP_ASSET_GRANTED_REF_COUNT_FIELD = '_asset_granted_ref_count'
TMP_GRANTED_REF_COUNT_FIELD = '_granted_ref_count'


def obj_field_add(obj, field, value=1):
    new_value = getattr(obj, field, 0) + value
    setattr(obj, field, new_value)


def inc_tmp_granted_ref_count(obj, value=1):
    obj_field_add(obj, TMP_GRANTED_REF_COUNT_FIELD, value)


def inc_tmp_asset_granted_ref_count(obj, value=1):
    obj_field_add(obj, TMP_ASSET_GRANTED_REF_COUNT_FIELD, value)


def update_mapping_nodes(mapping_node_keys, user, nodes: List[Node], action):
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
                    asset_granted_ref_count=_asset_granted_ref_count
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
                             Node.objects.filter(key__in=set(chain(ancestor_keys_groups)))}

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
