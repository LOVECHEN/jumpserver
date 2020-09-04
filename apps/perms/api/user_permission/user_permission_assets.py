# -*- coding: utf-8 -*-
#
from operator import or_
from functools import reduce

from django.db.models import Q
from django.utils.decorators import method_decorator
from perms.api.user_permission.mixin import UserGrantedNodeAssetMixin
from rest_framework.generics import ListAPIView

from common.utils import get_object_or_none
from users.models import User
from common.permissions import IsOrgAdminOrAppUser, IsValidUser
from common.utils import get_logger
from ...hands import Node
from ... import serializers
from .mixin import UserAssetTreeMixin
from perms.models import UserGrantedMappingNode
from assets.models import Asset
from orgs.utils import tmp_to_root_org


logger = get_logger(__name__)

__all__ = [
    'UserGrantedAssetsForAdminApi', 'UserGrantedAssetsAsTreeApi',
    'UserGrantedNodeAssetsApi', 'UserGrantedAssetsForUserApi'
]


class UserGrantedAssetsForAdminApi(ListAPIView):
    permission_classes = (IsOrgAdminOrAppUser,)
    serializer_class = serializers.AssetGrantedSerializer
    only_fields = serializers.AssetGrantedSerializer.Meta.only_fields
    filter_fields = ['hostname', 'ip', 'id', 'comment']
    search_fields = ['hostname', 'ip', 'comment']

    def get_user(self):
        return User.objects.get(id=self.kwargs.get('pk'))

    def get_queryset(self):
        user = self.get_user()

        return Asset.objects.filter(
            Q(granted_by_permissions__users=user) |
            Q(granted_by_permissions__user_groups__users=user)
        ).distinct().only(
            *self.only_fields
        )


@method_decorator(tmp_to_root_org(), name='list')
class UserGrantedAssetsForUserApi(UserGrantedAssetsForAdminApi):
    permission_classes = (IsValidUser,)

    def get_user(self):
        return self.request.user


@method_decorator(tmp_to_root_org(), name='list')
class UserGrantedAssetsAsTreeApi(UserAssetTreeMixin, UserGrantedAssetsForUserApi):
    pass


@method_decorator(tmp_to_root_org(), name='list')
class UserGrantedNodeAssetsApi(UserGrantedNodeAssetMixin, ListAPIView):
    permission_classes = (IsValidUser,)
    serializer_class = serializers.AssetGrantedSerializer
    only_fields = serializers.AssetGrantedSerializer.Meta.only_fields
    filter_fields = ['hostname', 'ip', 'id', 'comment']
    search_fields = ['hostname', 'ip', 'comment']

    def get_queryset(self):
        node_id = self.kwargs.get("node_id")
        user = self.request.user

        mapping_node: UserGrantedMappingNode = get_object_or_none(
            UserGrantedMappingNode, user=user, node_id=node_id)
        node = Node.objects.get(id=node_id)
        return self.dispatch_node_process(node.key, mapping_node, node)

    def on_granted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        return Asset.objects.filter(
            Q(nodes__key__startswith=f'{node.key}:') |
            Q(nodes__id=node.id)
        ).distinct()

    def on_ungranted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        user = self.request.user
        assets = Asset.objects.none()

        # 查询该节点下的授权节点
        granted_mapping_nodes = UserGrantedMappingNode.objects.filter(
            user=user,
            granted=True,
            key__startswith=f'{node.key}:',
        )

        # 根据授权节点构建查询
        granted_nodes_qs = []
        for node in granted_mapping_nodes:
            granted_nodes_qs.append(Q(nodes__key__startswith=f'{node.key}:'))
            granted_nodes_qs.append(Q(nodes__key=node.key))

        # 查询该节点下的资产授权节点
        only_asset_granted_mapping_nodes = UserGrantedMappingNode.objects.filter(
            user=user,
            asset_granted=True,
            granted=False,
            key__startswith=f'{node.key}:',
        )

        # 根据资产授权节点构建查询
        only_asset_granted_nodes_qs = []
        for node in only_asset_granted_mapping_nodes:
            only_asset_granted_nodes_qs.append(Q(nodes__id=node.node_id))

        # 判断当前节点有没有授权资产
        if mapping_node.asset_granted:
            only_asset_granted_nodes_qs.append(Q(nodes__id=node.id))

        q = []
        if granted_nodes_qs:
            q.append(reduce(or_, granted_nodes_qs))

        if only_asset_granted_nodes_qs:
            only_asset_granted_nodes_q = reduce(or_, only_asset_granted_nodes_qs)
            only_asset_granted_nodes_q &= Q(granted_by_permissions__users=user) | Q(
                granted_by_permissions__user_groups__users=user)
            q.append(only_asset_granted_nodes_q)

        if q:
            assets = Asset.objects.filter(reduce(or_, q)).distinct()
        return assets
