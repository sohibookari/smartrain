import time

from logging import Logger
from typing import Any, Dict

import smartrain.context as ctx
from smartrain import BasicConfig, DictReader

config: BasicConfig = ctx.get('config')
logger: Logger = config.getLogger()


def run_all_tasks(source) -> Dict[Any, Any]:
    maps = config.VIEW_TASK_MAP
    result_dict = {}
    for task_type in maps.keys():
        for task in maps[task_type]:
            reader = DictReader(source)
            result_dict[task] = run_single_task(task_type, task, reader)
    return result_dict


def run_single_task(task_type, task, reader):
    logger.info('run task %s as %s.' % (task, task_type))
    start_t = time.time()
    data = reader.read_view(task)
    result = getattr(ctx.get('mapper'), '_do_%s' % task_type)(data)
    end_t = time.time()
    logger.info('task %s successfully ended in %f seconds.' % (task, (end_t - start_t)))

    return result
