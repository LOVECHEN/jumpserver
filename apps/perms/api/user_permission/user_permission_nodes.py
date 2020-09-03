# -*- coding: utf-8 -*-
#
from itertools import chain

from django.shortcuts import get_object_or_404
from django.db.models import Q
from rest_framework.generics import (
    ListAPIView, get_object_or_404
)

from users.models import User
from common.permissions import IsOrgAdminOrAppUser, IsValidUser
from common.utils import get_logger
from ...hands import Node, NodeSerializer
from ... import serializers
from .mixin import UserNodeTreeMixin


logger = get_logger(__name__)

__all__ = [
    'UserGrantedNodesForAdminApi',
    'UserGrantedNodesForUserApi',
    'UserGrantedNodesAsTreeApi',
    'UserGrantedNodeChildrenApi',
    'UserGrantedNodeChildrenAsTreeApi',
]


class UserGrantedNodesForAdminApi(ListAPIView):
    """
    查询用户授权的所有节点的API
    """
    permission_classes = (IsOrgAdminOrAppUser,)
    serializer_class = serializers.NodeGrantedSerializer
    nodes_only_fields = NodeSerializer.Meta.only_fields

    def get_user(self):
        return User.objects.get(id=self.kwargs.get('pk'))

    def get_queryset(self):
        user = self.get_user()

        # 查询所有直接授权或者资产授权的节点
        queryset_by_user = Node.objects.filter(
            Q(granted_by_permissions__users=user) |
            Q(granted_by_permissions__user_groups__users=user)
        ).distinct().only(
            *self.nodes_only_fields
        )

        queryset_by_group = Node.objects.filter(
            Q(assets__granted_by_permissions__users=user) |
            Q(assets__granted_by_permissions__user_groups__users=user)
        ).distinct().only(
            *self.nodes_only_fields
        )

        leaf_nodes = [*queryset_by_user, *queryset_by_group]
        # 计算以上节点的祖先节点 key
        ancestor_keys = set()
        for node in leaf_nodes:
            ancestor_keys.update(node.get_ancestor_keys())
        ancestor_nodes = Node.objects.filter(key__in=ancestor_keys)
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


class UserGrantedNodesAsTreeApi(UserNodeTreeMixin, UserGrantedNodesForAdminApi):
    pass


class UserGrantedNodeChildrenApi(UserGrantedNodesForAdminApi):
    node = None
    root_keys = None  # 如果是第一次访问，则需要把二级节点添加进去，这个 roots_keys

    def get(self, request, *args, **kwargs):
        key = self.request.query_params.get("key")
        pk = self.request.query_params.get("id")

        node = None
        if pk is not None:
            node = get_object_or_404(Node, id=pk)
        elif key is not None:
            node = get_object_or_404(Node, key=key)
        self.node = node
        return super().get(request, *args, **kwargs)

    def get_queryset(self):
        if self.node:
            queryset = Node.objects.filter(parent_key=self.node.key)
        else:
            queryset = Node.objects.filter(parent_key='')
        return queryset


class UserGrantedNodeChildrenAsTreeApi(UserNodeTreeMixin, UserGrantedNodeChildrenApi):
    pass
