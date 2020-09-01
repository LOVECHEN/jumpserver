from typing import List

from assets.models import Node, Asset


class SerializeToTreeNodeMixin:
    permission_classes = ()

    def serialize_nodes(self, nodes: List[Node], with_asset_amount=False):
        if with_asset_amount:
            def _name(node: Node):
                return '{} ({})'.format(node.value, node.assets_amount)
        else:
            def _name(node: Node):
                return node.value

        data = [
            {
                'id': node.key,
                'name': _name(node),
                'title': _name(node),
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
