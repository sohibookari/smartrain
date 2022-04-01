import smartrain.context as ctx

from smartrain.constructor import TaskConfig, SmrTask


def run():
    for obj in ctx.lists():
        if obj.endswith('_task'):
            task_config: TaskConfig = ctx.get(obj)
            tasks = task_config.tasks
