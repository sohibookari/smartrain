import smartrain.context as ctx


def run():
    for obj in ctx.list():
        if obj.endswith('task@config'):
            task_config = ctx._get(obj)
            for task in task_config.tasks:
                for st in task:
                    st.run()
            break

