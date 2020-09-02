from uuid import uuid4
import threading

from django.db.transaction import atomic

from users.models import User
from common.thread_pools import SingletonThreadPoolExecutor
from common.utils import get_logger
from perms.models import UpdateMappingNodeTask
from perms.utils.user_node_tree import on_node_asset_change
from common.const.distributed_lock_key import UPDATE_MAPPING_NODE_TASK_LOCK_KEY
from common.utils.timezone import dt_formater, now
from orgs import lock
from assets.models import Node

logger = get_logger(__name__)


class Executor(SingletonThreadPoolExecutor):
    pass


executor = Executor()


VALUE_TEMPLATE = '{stage}:thread:{thread_name}:{thread_id}:{now}:{rand_str}'


def _generate_value(stage=lock.DOING):
    cur_thread = threading.current_thread()

    return VALUE_TEMPLATE.format(
        stage=stage,
        thread_name=cur_thread.name,
        thread_id=cur_thread.ident,
        now=dt_formater(now()),
        rand_str=uuid4()
    )


def run_user_mapping_node_task(user: User):
    key = UPDATE_MAPPING_NODE_TASK_LOCK_KEY.format(user_id=user.id)
    doing_value = _generate_value()
    commiting_value = _generate_value(stage=lock.COMMITING)

    try:
        lock.acquire(key, doing_value, timeout=60)
        if not lock:
            raise lock.SomeoneIsDoingThis

        with atomic(savepoint=False):
            tasks = UpdateMappingNodeTask.objects.filter(user=user).order_by('date_created')
            if tasks:
                to_delete = []
                for task in tasks:
                    nodes = Node.objects.filter(id__in=task.node_pks)
                    on_node_asset_change(user, nodes, len(task.asset_pks), task.action)
                    to_delete.append(task.id)
                UpdateMappingNodeTask.objects.filter(id__in=to_delete).delete()

                ok = lock.change_lock_state_to_commiting(key, doing_value, commiting_value)
                if not ok:
                    logger.error(f'update_mapping_node_task_timeout for user: {user.id}')
                    raise lock.Timeout
    finally:
        lock.release(key, commiting_value, doing_value)


def run_mapping_node_tasks():
    failed_user_ids = []

    logger.debug(f'mapping_node_tasks running')

    while True:
        task: UpdateMappingNodeTask = UpdateMappingNodeTask.objects.exclude(
            user_id__in=failed_user_ids
        ).first()

        if task is None:
            break

        user = task.user
        try:
            run_user_mapping_node_task(user)
        except:
            failed_user_ids.append(user.id)

    logger.debug(f'mapping_node_tasks finished')


def submit_update_mapping_node_task():
    executor.submit(run_mapping_node_tasks)
