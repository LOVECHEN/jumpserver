from assets.tasks.push_system_user import logger


def get_object_if_need(model, pk):
    if not isinstance(pk, model):
        try:
            return model.objects.get(id=pk)
        except model.DoesNotExist as e:
            logger.error(f'DoesNotExist: <{model.__name__}:{pk}> not exist')
            raise e
    return pk


def get_objects_if_need(model, pks):
    if not pks:
        return pks
    if not isinstance(pks[0], model):
        objs = list(model.objects.filter(id__in=pks))
        if len(objs) != len(pks):
            pks = set(pks)
            exists_pks = {o.id for o in objs}
            not_found_pks = ','.join(pks - exists_pks)
            logger.error(f'DoesNotExist: <{model.__name__}: {not_found_pks}>')
        return objs
    return pks
