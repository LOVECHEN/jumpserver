# -*- coding: utf-8 -*-
#
from itertools import chain

from django.shortcuts import get_object_or_404
from django.db.models import Q
from rest_framework.generics import (
    ListAPIView, get_object_or_404
)
from rest_framework.response import Response

from assets.api.mixin import SerializeToTreeNodeMixin
from users.models import User
from common.permissions import IsOrgAdminOrAppUser, IsValidUser
from common.utils import get_logger
from ...hands import Node, NodeSerializer
from ... import serializers


logger = get_logger(__name__)

__all__ = [
    'UserGrantedNodesForAdminApi',
    'UserGrantedNodesForUserApi',
    'MyGrantedNodesAsTreeApi',
    'UserGrantedNodeChildrenApi',
    'UserGrantedNodeChildrenAsTreeApi',
]


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

        # 查询所有直接授权的节点
        queryset_from_node = Node.objects.filter(
            Q(granted_by_permissions__users=user) |
            Q(granted_by_permissions__user_groups__users=user)
        ).distinct().only(
            *self.nodes_only_fields
        )

        # 查询所有资产授权的节点
        queryset_from_asset = Node.objects.filter(
            Q(assets__granted_by_permissions__users=user) |
            Q(assets__granted_by_permissions__user_groups__users=user)
        ).distinct().only(
            *self.nodes_only_fields
        )

        leaf_nodes = [*queryset_from_node, *queryset_from_asset]
        # 计算以上节点的祖先节点 key
        ancestor_keys = set()
        for node in leaf_nodes:
            ancestor_keys.update(node.get_ancestor_keys())

        # 查询所有祖先节点
        ancestor_nodes = Node.objects.filter(key__in=ancestor_keys).only(*self.nodes_only_fields)
        nodes = []
        exist_keys = set()
        for node in chain(leaf_nodes, ancestor_nodes):
            if node.key not in exist_keys:
                exist_keys.add(node.key)
                nodes.append(node)
        return nodes


class UserGrantedNodesForUserApi(UserGrantedNodesForAdminApi):
    permission_classes = (IsValidUser,)

    def get_user(self):
        return self.request.user


class MyGrantedNodesAsTreeApi(SerializeToTreeNodeMixin, UserGrantedNodesForAdminApi):
    def list(self, request, *args, **kwargs):
        queryset = self.get_queryset()
        data = self.serialize_nodes(queryset, with_asset_amount=True)
        return Response(data=data)


class UserGrantedNodeChildrenApi(UserGrantedNodesForAdminApi):
    def get_queryset(self):
        key = self.request.query_params.get("key")
        pk = self.request.query_params.get("id")

        if key is None and pk:
            key = Node.objects.get(id=pk).key

        if key is not None:
            queryset = Node.objects.filter(parent_key=key)
        else:
            queryset = Node.objects.filter(parent_key='')
        return queryset


class UserGrantedNodeChildrenAsTreeApi(UserGrantedNodeChildrenApi):
    pass
