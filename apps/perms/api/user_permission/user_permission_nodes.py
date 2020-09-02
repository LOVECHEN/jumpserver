# -*- coding: utf-8 -*-
#

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
        queryset = Node.objects.filter(
            Q(granted_by_permissions__users=user) |
            Q(granted_by_permissions__user_groups__users=user)
        ).distinct().only(
            *self.nodes_only_fields
        )
        return queryset


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
            children = self.tree.children(self.node.key)
        else:
            children = self.tree.children(self.tree.root)
            # 默认打开组织节点下的节点
            self.root_keys = [child.identifier for child in children]
            for key in self.root_keys:
                children.extend(self.tree.children(key))
        node_keys = [n.identifier for n in children]
        queryset = Node.objects.filter(key__in=node_keys)
        return queryset


class UserGrantedNodeChildrenAsTreeApi(UserNodeTreeMixin, UserGrantedNodeChildrenApi):
    pass
