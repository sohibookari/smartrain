import smartrain.context as ctx


def run():
    tl = ctx.list().copy()
    for obj in tl:
        if obj.endswith('task@config'):
            task_config = ctx._get(obj)
            for task in task_config.tasks:
                for st in task:
                    st.run()
