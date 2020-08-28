from rest_framework.pagination import LimitOffsetPagination

from assets.models import Node
from common.utils import dict_get_any, is_uuid, get_object_or_none


class AssetLimitOffsetPagination(LimitOffsetPagination):
    def get_count(self, queryset):
        node_id = dict_get_any(self._request.query_params, ['node', 'node_id'])
        node = None
        if node_id:
            if is_uuid(node_id):
                node = get_object_or_none(Node, id=node_id)
            else:
                node = get_object_or_none(Node, key=node_id)
        if not node:
            node = Node.org_root()
        return node.assets_amount

    def paginate_queryset(self, queryset, request, view=None):
        self._request = request
        return super(AssetLimitOffsetPagination, self).paginate_queryset(queryset, request, view=None)
