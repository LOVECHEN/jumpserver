# -*- coding: utf-8 -*-
#
from typing import List

from rest_framework.generics import ListAPIView
from rest_framework.request import Request
from rest_framework.response import Response
from django.db.models import Q, F
from django.utils.decorators import method_decorator

from common.permissions import IsValidUser
from common.utils.django import get_object_or_none
from common.utils import get_logger
from .user_permission_nodes import MyGrantedNodesAsTreeApi
from .user_permission_nodes import UserGrantedNodeChildrenAsTreeApi
from .mixin import UserGrantedNodeAssetMixin
from perms.models import UserGrantedMappingNode
from perms.utils.user_node_tree import (
    TMP_GRANTED_FIELD, TMP_GRANTED_ASSET_AMOUNT, node_annotate_mapping_node,
    is_asset_granted, is_granted, get_granted_asset_amount, node_annotate_set_granted,
    get_granted_q,
)

from assets.models import Asset
from assets.api import SerializeToTreeNodeMixin
from orgs.utils import tmp_to_root_org
from ...hands import Node

logger = get_logger(__name__)

__all__ = [
    'MyGrantedNodesAsTreeApi',
    'UserGrantedNodeChildrenAsTreeApi',
    'UserGrantedNodeChildrenWithAssetsAsTreeApi',
    'UserGrantedNodeChildrenApi',
    'MyGrantedNodesWithAssetsAsTreeApi',
]


class MyGrantedNodesWithAssetsAsTreeApi(SerializeToTreeNodeMixin, ListAPIView):
    permission_classes = (IsValidUser,)

    @tmp_to_root_org()
    def list(self, request: Request, *args, **kwargs):
        user = request.user

        # 获取 `UserGrantedMappingNode` 中对应的 `Node`
        nodes = Node.objects.filter(
            mapping_nodes__user=user,
        ).annotate(**node_annotate_mapping_node).distinct()

        key2nodes_mapper = {}
        descendant_q = Q()
        granted_q = Q()

        for _node in nodes:
            if not is_granted(_node):
                _node.assets_amount = get_granted_asset_amount(_node)
            else:
                # 直接授权的节点
                granted_q |= Q(nodes__key__startswith=f'{_node.key}:')
                granted_q |= Q(nodes__key=_node.key)
                descendant_q |= Q(key__startswith=f'{_node.key}:')
            key2nodes_mapper[_node.key] = _node

        if descendant_q:
            descendant_nodes = Node.objects.filter(descendant_q).annotate(**node_annotate_set_granted)
            for _node in descendant_nodes:
                key2nodes_mapper[_node.key] = _node

        all_nodes = key2nodes_mapper.values()

        # 查询出所有资产
        all_assets = Asset.objects.filter(
            get_granted_q(user) |
            granted_q
        ).annotate(parent_key=F('nodes__key')).distinct()

        data = [
            *self.serialize_nodes(all_nodes, with_asset_amount=True),
            *self.serialize_assets(all_assets)
        ]
        return Response(data=data)


@method_decorator(tmp_to_root_org(), name='list')
class UserGrantedNodeChildrenWithAssetsAsTreeApi(UserGrantedNodeAssetMixin, SerializeToTreeNodeMixin, ListAPIView):
    permission_classes = (IsValidUser, )

    def on_granted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        nodes = Node.objects.filter(parent_key=key)
        assets = Asset.objects.filter(nodes__key=key).distinct()
        return nodes, assets

    def on_ungranted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        user = self.request.user
        assets = Asset.objects.none()
        nodes = Node.objects.filter(
            parent_key=key,
            mapping_nodes__user=user,
        ).annotate(
            _granted_asset_amount=F('mapping_nodes__assets_amount'),
            _granted=F('mapping_nodes__granted')
        ).distinct()

        # TODO 可配置
        for _node in nodes:
            if not getattr(_node, TMP_GRANTED_FIELD, False):
                _node.assets_amount = getattr(_node, TMP_GRANTED_ASSET_AMOUNT, 0)

        if mapping_node.asset_granted:
            assets = Asset.objects.filter(
                nodes__key=key,
            ).filter(Q(granted_by_permissions__users=user) | Q(granted_by_permissions__user_groups__users=user))
        return nodes, assets

    def list(self, request: Request, *args, **kwargs):
        user = request.user
        key = request.query_params.get('key')
        self.submit_update_mapping_node_task(user)

        nodes = []
        assets = []
        if not key:
            root_nodes = Node.objects.filter(
                mapping_nodes__user=user, parent_key=''
            )
            nodes.extend(root_nodes)
        else:
            mapping_node: UserGrantedMappingNode = get_object_or_none(
                UserGrantedMappingNode, user=user, key=key)
            nodes, assets = self.dispatch_node_process(key, mapping_node)
        nodes = self.serialize_nodes(nodes, with_asset_amount=True)
        assets = self.serialize_assets(assets, key)
        return Response(data=[*nodes, *assets])


@method_decorator(tmp_to_root_org(), name='list')
class UserGrantedNodeChildrenApi(UserGrantedNodeAssetMixin, SerializeToTreeNodeMixin, ListAPIView):
    permission_classes = (IsValidUser, )

    def list(self, request: Request, *args, **kwargs):

        user = request.user
        key = request.query_params.get('key')

        self.submit_update_mapping_node_task(user)

        if not key:
            nodes = Node.objects.filter(
                mapping_nodes__user=user,
                parent_key=''
            ).annotate(
                _granted_asset_amount=F('mapping_nodes__assets_amount'),
                _granted=F('mapping_nodes__granted')
            ).distinct()

            # TODO 可配置
            for _node in nodes:
                if not getattr(_node, TMP_GRANTED_FIELD, False):
                    _node.assets_amount = getattr(_node, TMP_GRANTED_ASSET_AMOUNT, 0)
        else:
            mapping_node = get_object_or_none(
                UserGrantedMappingNode, user=user, key=key
            )
            nodes = self.dispatch_node_process(key, mapping_node, None)
        nodes = self.serialize_nodes(nodes, with_asset_amount=True)
        return Response(data=nodes)

    def on_granted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        return Node.objects.filter(parent_key=key)

    def on_ungranted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        user = self.request.user
        nodes = Node.objects.filter(
            parent_key=key,
            mapping_nodes__user=user,
        ).annotate(
            _granted_asset_amount=F('mapping_nodes__assets_amount'),
            _granted=F('mapping_nodes__granted')
        ).distinct()

        # TODO 可配置
        for _node in nodes:
            if not getattr(_node, TMP_GRANTED_FIELD, False):
                _node.assets_amount = getattr(_node, TMP_GRANTED_ASSET_AMOUNT, 0)

        return nodes
