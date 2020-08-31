# -*- coding: utf-8 -*-
#
from typing import List

from rest_framework.generics import ListAPIView
from rest_framework.request import Request
from rest_framework.response import Response

from common.utils.django import get_object_or_none
from common.utils import get_logger
from ...utils import ParserNode
from .mixin import UserAssetTreeMixin
from .user_permission_nodes import UserGrantedNodesAsTreeApi
from .user_permission_nodes import UserGrantedNodeChildrenAsTreeApi
from perms.models import MappingNode
from assets.models import Node, Asset


logger = get_logger(__name__)

__all__ = [
    'UserGrantedNodesAsTreeApi',
    'UserGrantedNodesWithAssetsAsTreeApi',
    'UserGrantedNodeChildrenAsTreeApi',
    'UserGrantedNodeChildrenWithAssetsAsTreeApi',
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


class UserGrantedNodeChildrenWithAssetsAsTreeApi(ListAPIView):
    nodes_only_fields = ParserNode.nodes_only_fields
    assets_only_fields = ParserNode.assets_only_fields

    def serialize_nodes(self, nodes: List[Node]):
        data = [
            {
                'id': node.key,
                'name': node.value,
                'title': node.value,
                'pId': node.parent_key,
                'isParent': True,
                'open': node.is_org_root(),
                'meta': {
                    'node': {
                        "id": node.id,
                        "key": node.key,
                        "value": node.value,
                    },
                    'type': 'node'
                }
            }
            for node in nodes
        ]
        return data

    def get_platform(self, asset: Asset):
        default = 'file'
        icon = {'windows', 'linux'}
        platform = asset.platform_base.lower()
        if platform in icon:
            return platform
        return default

    def serialize_assets(self, assets, node_key):
        data = [
            {
                'id': str(asset.id),
                'name': asset.hostname,
                'title': asset.ip,
                'pId': node_key,
                'isParent': False,
                'open': False,
                'iconSkin': self.get_platform(asset),
                'nocheck': not asset.has_protocol('ssh'),
                'meta': {
                    'type': 'asset',
                    'asset': {
                        'id': asset.id,
                        'hostname': asset.hostname,
                        'ip': asset.ip,
                        'protocols': asset.protocols_as_list,
                        'platform': asset.platform_base,
                        'domain': asset.domain_id,
                        'org_name': asset.org_name,
                        'org_id': asset.org_id
                    },
                }
            }
            for asset in assets
        ]
        return data

    def list(self, request: Request, *args, **kwargs):
        user = request.user
        key = request.query_params.get('key')

        nodes = []
        assets = []
        if not key:
            root_node = Node.objects.filter(
                mapping_nodes__user=user,
                granted_ref_count__gt=0
            ).get(parent_key='')
            nodes.append(root_node)
        else:
            mapping_node: MappingNode = get_object_or_none(
                MappingNode, user=user, key=key, granted_ref_count__gt=0)
            if mapping_node is None:
                nodes = Node.objects.filter(parent_key=key)
                assets = Asset.objects.filter(nodes__key=key).distinct()
            else:
                if mapping_node.granted:
                    nodes = Node.objects.filter(parent_key=key)
                    assets = Asset.objects.filter(nodes__key=key).distinct()
                else:
                    nodes = Node.objects.filter(
                        mapping_nodes__parent_key=key,
                        mapping_nodes__user=user,
                        mapping_nodes__granted_ref_count__gt=0
                    ).distinct()
                    if mapping_node.asset_granted_ref_count > 0:
                        pass

        data = self.serialize_nodes(nodes)
        self.serialize_assets(assets, key)
        return data
