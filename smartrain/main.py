import smartrain.context as ctx


def run():
    tl = ctx.get_keys().copy()
    logger = ctx.get('logger')
    for obj in tl:
        if obj.endswith('task@config'):
            task_config = ctx._get(obj)
            for task in task_config.tasks:
                if task[0] == '^Skip':
                    continue
                for st in task:
                    if st.skip:
                        logger.info('sub task %s skipped.')
                        continue
                    st.run()
