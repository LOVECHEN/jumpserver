# -*- coding: utf-8 -*-
#


from . import user_permission as uapi
from .mixin import UserGroupPermissionMixin

__all__ = [
    'UserGroupGrantedAssetsApi', 'UserGroupGrantedNodesApi',
    'UserGroupGrantedNodeAssetsApi', 'UserGroupGrantedNodeChildrenApi',
    'UserGroupGrantedNodeChildrenAsTreeApi',
    'UserGroupGrantedNodeChildrenWithAssetsAsTreeApi',
    'UserGroupGrantedAssetSystemUsersApi',
    # 'UserGroupGrantedNodeChildrenWithAssetsAsTreeApi',
]


class UserGroupGrantedAssetsApi(UserGroupPermissionMixin, uapi.UserGrantedAssetsForAdminApi):
    pass


class UserGroupGrantedNodeAssetsApi(UserGroupPermissionMixin, uapi.UserGrantedNodeAssetsForAdminApi):
    pass


class UserGroupGrantedNodesApi(UserGroupPermissionMixin, uapi.UserGrantedNodesForAdminApi):
    pass


class UserGroupGrantedNodeChildrenApi(UserGroupPermissionMixin, uapi.UserGrantedNodeChildrenApi):
    pass


class UserGroupGrantedNodeChildrenAsTreeApi(UserGroupPermissionMixin, uapi.UserGrantedNodeChildrenAsTreeApi):
    pass


class UserGroupGrantedNodeChildrenWithAssetsAsTreeApi(UserGroupPermissionMixin, uapi.UserGrantedNodeChildrenWithAssetsAsTreeForAdminApi):
    pass


class UserGroupGrantedAssetSystemUsersApi(UserGroupPermissionMixin, uapi.UserGrantedAssetSystemUsersForAdminApi):
    pass

