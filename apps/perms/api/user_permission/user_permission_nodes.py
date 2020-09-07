# -*- coding: utf-8 -*-
#
from itertools import chain

from django.utils.decorators import method_decorator
from django.db.models import Q, F
from rest_framework.generics import (
    ListAPIView
)
from rest_framework.response import Response

from perms.utils.user_node_tree import (
    TMP_GRANTED_FIELD, TMP_GRANTED_ASSET_AMOUNT, node_annotate_mapping_node,
    is_granted, get_granted_asset_amount, node_annotate_set_granted,
)
from common.utils.django import get_object_or_none
from common.utils import lazyproperty
from perms.models import UserGrantedMappingNode
from orgs.utils import tmp_to_root_org
from assets.api.mixin import SerializeToTreeNodeMixin
from users.models import User
from common.permissions import IsOrgAdminOrAppUser, IsValidUser
from common.utils import get_logger
from ...hands import Node, NodeSerializer
from .mixin import UserGrantedNodeAssetMixin
from ... import serializers


logger = get_logger(__name__)

__all__ = [
    'UserGrantedNodesForAdminApi',
    'UserGrantedNodesForUserApi',
    'MyGrantedNodesAsTreeApi',
    'UserGrantedNodeChildrenApi',
    'UserGrantedNodeChildrenForAdminApi',
    'MyGrantedNodeChildrenApi',
]


@method_decorator(tmp_to_root_org(), name='list')
class UserGrantedNodesForAdminApi(ListAPIView):
    """
    查询用户授权的所有节点
    """
    permission_classes = (IsOrgAdminOrAppUser,)
    serializer_class = serializers.NodeGrantedSerializer
    nodes_only_fields = NodeSerializer.Meta.only_fields

    def get_user(self):
        return User.objects.get(id=self.kwargs.get('pk'))

    def get_queryset(self):
        user = self.get_user()

        # 获取 `UserGrantedMappingNode` 中对应的 `Node`
        nodes = Node.objects.filter(
            mapping_nodes__user=user,
        ).annotate(**node_annotate_mapping_node).distinct()

        key2nodes_mapper = {}
        descendant_q = Q()

        for _node in nodes:
            if not is_granted(_node):
                # 未授权的节点资产数量设置为 `UserGrantedMappingNode` 中的数量
                _node.assets_amount = get_granted_asset_amount(_node)
            else:
                # 直接授权的节点
                # 增加查询后代节点的过滤条件
                descendant_q |= Q(key__startswith=f'{_node.key}:')

            key2nodes_mapper[_node.key] = _node

        if descendant_q:
            descendant_nodes = Node.objects.filter(descendant_q).annotate(**node_annotate_set_granted)
            for _node in descendant_nodes:
                key2nodes_mapper[_node.key] = _node

        all_nodes = key2nodes_mapper.values()
        return all_nodes


class UserGrantedNodesForUserApi(UserGrantedNodesForAdminApi):
    permission_classes = (IsValidUser,)

    def get_user(self):
        return self.request.user


class MyGrantedNodesAsTreeApi(SerializeToTreeNodeMixin, UserGrantedNodesForAdminApi):
    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        data = self.serialize_nodes(queryset, with_asset_amount=True)
        return Response(data=data)


class UserGrantedNodeChildrenBaseApi(UserGrantedNodeAssetMixin, ListAPIView):
    for_admin = False

    @lazyproperty
    def user(self):
        if self.for_admin:
            user_id = self.kwargs.get('pk')
            return User.objects.get(id=user_id)
        else:
            return self.request.user

    def get_nodes(self):
        user = self.user
        key = self.request.query_params.get('key')

        self.submit_update_mapping_node_task(user)

        if not key:
            nodes = Node.objects.filter(
                mapping_nodes__user=user,
                parent_key=''
            ).annotate(
                _granted_asset_amount=F('mapping_nodes__assets_amount'),
                _granted=F('mapping_nodes__granted')
            ).distinct()

            # 设置节点授权资产数量
            for _node in nodes:
                if not getattr(_node, TMP_GRANTED_FIELD, False):
                    _node.assets_amount = getattr(_node, TMP_GRANTED_ASSET_AMOUNT, 0)
        else:
            mapping_node = get_object_or_none(
                UserGrantedMappingNode, user=user, key=key
            )
            nodes = self.dispatch_node_process(key, mapping_node, None)
        return nodes

    def on_granted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        return Node.objects.filter(parent_key=key)

    def on_ungranted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        user = self.user

        nodes = Node.objects.filter(
            parent_key=key,
            mapping_nodes__user=user,
        ).annotate(
            _granted_asset_amount=F('mapping_nodes__assets_amount'),
            _granted=F('mapping_nodes__granted')
        ).distinct()

        # 设置节点授权资产数量
        for _node in nodes:
            if not getattr(_node, TMP_GRANTED_FIELD, False):
                _node.assets_amount = getattr(_node, TMP_GRANTED_ASSET_AMOUNT, 0)

        return nodes


class UserGrantedNodeChildrenApi(UserGrantedNodeChildrenBaseApi):
    serializer_class = serializers.NodeGrantedSerializer

    @tmp_to_root_org()
    def list(self, request, *args, **kwargs):
        nodes = self.get_nodes()
        serializer = self.get_serializer(nodes, many=True)
        return Response(serializer.data)


class UserGrantedNodeChildrenAsTreeApi(SerializeToTreeNodeMixin, UserGrantedNodeChildrenBaseApi):
    @tmp_to_root_org()
    def list(self, request, *args, **kwargs):
        nodes = self.get_nodes()
        nodes = self.serialize_nodes(nodes, with_asset_amount=True)
        return Response(data=nodes)


class ForAdminMixin:
    for_admin = True
    permission_classes = (IsOrgAdminOrAppUser,)


class ForUserMixin:
    for_admin = False
    permission_classes = (IsOrgAdminOrAppUser,)


class UserGrantedNodeChildrenForAdminApi(ForAdminMixin, UserGrantedNodeChildrenApi):
    pass


class MyGrantedNodeChildrenApi(ForUserMixin, UserGrantedNodeChildrenApi):
    pass


class UserGrantedNodeChildrenAsTreeForAdminApi(ForAdminMixin, UserGrantedNodeChildrenAsTreeApi):
    pass


class MyGrantedNodeChildrenAsTreeApi(ForUserMixin, UserGrantedNodeChildrenAsTreeApi):
    pass
