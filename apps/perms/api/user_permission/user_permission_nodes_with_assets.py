# -*- coding: utf-8 -*-
#
from rest_framework.generics import ListAPIView
from rest_framework.request import Request
from rest_framework.response import Response

from common.exceptions import SomeoneIsDoingThis
from common.utils.django import get_object_or_none
from common.utils import get_logger
from ...utils import ParserNode
from .mixin import UserAssetTreeMixin
from .user_permission_nodes import UserGrantedNodesAsTreeApi
from .user_permission_nodes import UserGrantedNodeChildrenAsTreeApi
from perms.models import MappingNode, UpdateMappingNodeTask
from perms.utils.user_node_tree import on_node_asset_change
from perms.utils import check_user_mapping_node_task
from assets.models import Node
from assets.api import SerializeToTreeNodeMixin
from perms.exceptions import AdminIsModifyingPerm
from orgs.utils import tmp_to_root_org

logger = get_logger(__name__)

__all__ = [
    'UserGrantedNodesAsTreeApi',
    'UserGrantedNodesWithAssetsAsTreeApi',
    'UserGrantedNodeChildrenAsTreeApi',
    'UserGrantedNodeChildrenWithAssetsAsTreeApi',
    'UserGrantedNodeChildrenApi',
]


class UserGrantedNodesWithAssetsAsTreeApi(UserGrantedNodesAsTreeApi):
    assets_only_fields = ParserNode.assets_only_fields

    def get_serializer_queryset(self, queryset):
        _queryset = super().get_serializer_queryset(queryset)
        _all_assets = self.util.get_assets().only(*self.assets_only_fields)
        _all_assets_map = {a.id: a for a in _all_assets}
        for node in queryset:
            assets_ids = self.tree.assets(node.key)
            assets = [_all_assets_map[_id] for _id in assets_ids if _id in _all_assets_map]
            _queryset.extend(
                UserAssetTreeMixin.parse_assets_to_queryset(assets, node)
            )
        return _queryset


class UserGrantedNodeChildrenWithAssetsAsTreeApi(SerializeToTreeNodeMixin, ListAPIView):
    permission_classes = ()

    def list(self, request: Request, *args, **kwargs):
        user = request.user
        key = request.query_params.get('key')
        try:
            run_user_mapping_node_task(user)
        except SomeoneIsDoingThis:
            raise AdminIsModifyingPerm

        nodes = []
        assets = []
        if not key:
            root_node = Node.objects.filter(
                mapping_nodes__user=user,
                mapping_nodes__granted_ref_count__gt=0
            ).get(parent_key='')
            nodes.append(root_node)
        else:
            mapping_node: MappingNode = get_object_or_none(
                MappingNode, user=user, key=key, granted_ref_count__gt=0)
            if mapping_node is None:
                nodes = Node.objects.filter(parent_key=key)
                # assets = Asset.objects.filter(nodes__key=key).distinct()
            else:
                if mapping_node.granted:
                    nodes = Node.objects.filter(parent_key=key)
                    # assets = Asset.objects.filter(nodes__key=key).distinct()
                else:
                    nodes = Node.objects.filter(
                        mapping_nodes__parent_key=key,
                        mapping_nodes__user=user,
                        mapping_nodes__granted_ref_count__gt=0
                    ).distinct()
                    # if mapping_node.asset_granted_ref_count > 0:
                    #     assets = Asset.objects.filter(
                    #         nodes__key=key,
                    #     ).filter(Q(granted_by_permissions__users=user)|Q(granted_by_permissions__user_groups__users=user))

        nodes = self.serialize_nodes(nodes)
        # assets = self.serialize_assets(assets, key)
        return Response(data=nodes)


class UserGrantedNodeChildrenApi(SerializeToTreeNodeMixin, ListAPIView):
    permission_classes = ()

    @tmp_to_root_org()
    def list(self, request: Request, *args, **kwargs):

        user = request.user
        key = request.query_params.get('key')

        try:
            check_user_mapping_node_task(user)
        except SomeoneIsDoingThis:
            raise AdminIsModifyingPerm

        if not key:
            nodes = Node.objects.filter(
                mapping_nodes__user=user,
                mapping_nodes__granted_ref_count__gt=0,
                parent_key=''
            )
        else:
            mapping_node = get_object_or_none(
                MappingNode, user=user, key=key, granted_ref_count__gt=0)
            if mapping_node is None:
                nodes = Node.objects.filter(parent_key=key)
            else:
                if mapping_node.granted:
                    nodes = Node.objects.filter(parent_key=key)
                else:
                    nodes = Node.objects.filter(
                        mapping_nodes__parent_key=key,
                        mapping_nodes__user=user,
                        mapping_nodes__granted_ref_count__gt=0
                    ).distinct()

        nodes = self.serialize_nodes(nodes)
        return Response(data=nodes)
