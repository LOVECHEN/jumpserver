# -*- coding: utf-8 -*-
#
from operator import or_
from functools import reduce

from django.shortcuts import get_object_or_404
from django.conf import settings
from django.db.models import Q
from rest_framework.generics import ListAPIView

from common.permissions import IsOrgAdminOrAppUser, IsValidUser
from common.utils import get_logger
from common.utils.django import get_object_or_none
from ...hands import Node
from ... import serializers
from .mixin import UserAssetPermissionMixin, UserAssetTreeMixin
from perms.models import MappingNode
from assets.models import Asset


logger = get_logger(__name__)

__all__ = [
    'UserGrantedAssetsApi', 'UserGrantedAssetsAsTreeApi',
    'UserGrantedNodeAssetsApi',
]


class UserGrantedAssetsApi(UserAssetPermissionMixin, ListAPIView):
    permission_classes = (IsOrgAdminOrAppUser,)
    serializer_class = serializers.AssetGrantedSerializer
    only_fields = serializers.AssetGrantedSerializer.Meta.only_fields
    filter_fields = ['hostname', 'ip', 'id', 'comment']
    search_fields = ['hostname', 'ip', 'comment']

    def filter_by_nodes(self, queryset):
        node_id = self.request.query_params.get("node")
        if not node_id:
            return queryset
        node = get_object_or_404(Node, pk=node_id)
        query_all = self.request.query_params.get("all", "0") in ["1", "true"]
        if query_all:
            pattern = '^{0}$|^{0}:'.format(node.key)
            queryset = queryset.filter(nodes__key__regex=pattern).distinct()
        else:
            queryset = queryset.filter(nodes=node)
        return queryset

    def filter_queryset(self, queryset):
        queryset = super().filter_queryset(queryset)
        queryset = self.filter_by_nodes(queryset)
        return queryset

    def get_queryset(self):
        queryset = self.util.get_assets().only(*self.only_fields).order_by(
            settings.TERMINAL_ASSET_LIST_SORT_BY
        )
        return queryset


class UserGrantedAssetsAsTreeApi(UserAssetTreeMixin, UserGrantedAssetsApi):
    pass


class UserGrantedNodeAssetsApi(ListAPIView):
    permission_classes = (IsValidUser,)
    serializer_class = serializers.AssetGrantedSerializer
    only_fields = serializers.AssetGrantedSerializer.Meta.only_fields
    filter_fields = ['hostname', 'ip', 'id', 'comment']
    search_fields = ['hostname', 'ip', 'comment']

    def get_queryset(self):
        node_id = self.kwargs.get("node_id")
        user = self.request.user

        assets = Asset.objects.none()

        mapping_node: MappingNode = get_object_or_none(
            MappingNode, user=user, node_id=node_id, granted_ref_count__gt=0)
        node = Node.objects.get(id=node_id)
        if mapping_node is None:
            assets = Asset.objects.filter(
                Q(nodes__key__startswith=f'{node.key}:') |
                Q(nodes__id=node_id)
            ).distinct()
        else:
            if mapping_node.granted:
                assets = Asset.objects.filter(
                    Q(nodes__key__startswith=f'{node.key}:') |
                    Q(nodes__id=node_id)
                ).distinct()
            else:
                granted_mapping_nodes = MappingNode.objects.filter(
                    granted=True,
                    granted_ref_count__gt=0,
                    key__startswith=f'{node.key}:',
                )

                granted_nodes_qs = []
                for node in granted_mapping_nodes:
                    granted_nodes_qs.append(Q(nodes__key__startswith=f'{node.key}:'))
                    granted_nodes_qs.append(Q(nodes__key=node.key))

                only_asset_granted_mapping_nodes = MappingNode.objects.filter(
                    granted=False,
                    asset_granted_ref_count__gt=0,
                    key__startswith=f'{node.key}:',
                )

                only_asset_granted_nodes_qs = []
                for node in only_asset_granted_mapping_nodes:
                    only_asset_granted_nodes_qs.append(Q(nodes__id=node.node_id))

                if mapping_node.asset_granted_ref_count > 0:
                    only_asset_granted_nodes_qs.append(Q(nodes__id=node_id))

                q = []
                if granted_nodes_qs:
                    q.append(reduce(or_, granted_nodes_qs))

                if only_asset_granted_nodes_qs:
                    only_asset_granted_nodes_q = reduce(or_, only_asset_granted_nodes_qs)
                    only_asset_granted_nodes_q &= Q(granted_by_permissions__users=user) | Q(granted_by_permissions__user_groups__users=user)
                    q.append(only_asset_granted_nodes_q)

                if q:
                    assets = Asset.objects.filter(reduce(or_, q)).distinct()

        return assets
