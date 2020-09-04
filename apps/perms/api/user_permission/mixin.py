# -*- coding: utf-8 -*-
#
from common.utils import lazyproperty
from common.tree import TreeNodeSerializer
from django.db.models import QuerySet
from perms.models import UserGrantedMappingNode
from rest_framework.exceptions import PermissionDenied

from perms.async_tasks.mapping_node_task import submit_update_mapping_node_task_for_user
from perms.exceptions import AdminIsModifyingPerm
from common.exceptions import SomeoneIsDoingThis
from ..mixin import UserPermissionMixin
from ...utils import AssetPermissionUtil, ParserNode
from ...hands import Node, Asset


class UserAssetPermissionMixin(UserPermissionMixin):
    util = None

    def get_cache_policy(self):
        return self.request.query_params.get('cache_policy', '0')

    @lazyproperty
    def util(self):
        cache_policy = self.get_cache_policy()
        system_user_id = self.request.query_params.get("system_user")
        util = AssetPermissionUtil(self.obj, cache_policy=cache_policy)
        if system_user_id:
            util.filter_permissions(system_users=system_user_id)
        return util

    @lazyproperty
    def tree(self):
        return self.util.get_user_tree()


class UserNodeTreeMixin:
    serializer_class = TreeNodeSerializer
    nodes_only_fields = ParserNode.nodes_only_fields

    def parse_nodes_to_queryset(self, nodes):
        if isinstance(nodes, QuerySet):
            nodes = nodes.only(*self.nodes_only_fields)
        _queryset = []

        for node in nodes:
            assets_amount = self.tree.valid_assets_amount(node.key)
            if assets_amount == 0 and not node.key.startswith('-'):
                continue
            node.assets_amount = assets_amount
            data = ParserNode.parse_node_to_tree_node(node)
            _queryset.append(data)
        return _queryset

    def get_serializer_queryset(self, queryset):
        queryset = self.parse_nodes_to_queryset(queryset)
        return queryset

    def get_serializer(self, queryset=None, many=True, **kwargs):
        if queryset is None:
            queryset = Node.objects.none()
        queryset = self.get_serializer_queryset(queryset)
        queryset.sort()
        return super().get_serializer(queryset, many=many, **kwargs)


class UserAssetTreeMixin:
    serializer_class = TreeNodeSerializer
    nodes_only_fields = ParserNode.assets_only_fields

    @staticmethod
    def parse_assets_to_queryset(assets, node):
        _queryset = []
        for asset in assets:
            data = ParserNode.parse_asset_to_tree_node(node, asset)
            _queryset.append(data)
        return _queryset

    def get_serializer_queryset(self, queryset):
        queryset = queryset.only(*self.nodes_only_fields)
        _queryset = self.parse_assets_to_queryset(queryset, None)
        return _queryset

    def get_serializer(self, queryset=None, many=True, **kwargs):
        if queryset is None:
            queryset = Asset.objects.none()
        queryset = self.get_serializer_queryset(queryset)
        queryset.sort()
        return super().get_serializer(queryset, many=many, **kwargs)


class UserGrantedNodeAssetMixin:

    def list(self, request, *args, **kwargs):
        self.submit_update_mapping_node_task(user=self.request.user)
        return super().list(request, *args, **kwargs)

    def submit_update_mapping_node_task(self, user):
        submit_update_mapping_node_task_for_user(user)

    def dispatch_node_process(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        if mapping_node is None:
            ancestor_keys = Node.get_node_ancestor_keys(key)
            granted = UserGrantedMappingNode.objects.filter(key__in=ancestor_keys, granted=True).exists()
            if not granted:
                raise PermissionDenied
            queryset = self.on_granted_node(key, mapping_node, node)
        else:
            if mapping_node.granted:
                # granted_node
                queryset = self.on_granted_node(key, mapping_node, node)
            else:
                queryset = self.on_ungranted_node(key, mapping_node, node)
        return queryset

    def on_granted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        raise NotImplementedError

    def on_ungranted_node(self, key, mapping_node: UserGrantedMappingNode, node: Node = None):
        raise NotImplementedError
