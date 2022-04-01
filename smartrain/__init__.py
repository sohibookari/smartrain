import os

import yaml
import smartrain.context as ctx

from smartrain.mapper import Mapper
from smartrain.constructor import basic_config_constructor, task_config_constructor, task_constructor


def load_configs():
    yaml_files = [name for name in os.listdir('.\\') if name.endswith('.yaml') or name.endswith('.yml')]
    for file in yaml_files:
        with open('.\\' + file) as f:
            try:
                cfg = yaml.load(f, Loader=yaml.FullLoader)
                if isinstance(cfg, dict):
                    list(map(lambda i: ctx.set(i[0], i[1]), cfg.items()))
                else:
                    ctx.set(file.split('.')[0], cfg)
            except yaml.YAMLError as e:
                print(e)


def load_objects():
    ctx.set('mapper', Mapper())


def list_context():
    ctx.get('config').getLogger().info('loaded context object: ' + [i for i in ctx.lists()].__str__())
    ctx.get('config').getLogger().info('smart-rain has been successful loaded.')


yaml.add_constructor('!BasicConfig', basic_config_constructor)
yaml.add_constructor('!TaskConfig', task_config_constructor)
yaml.add_constructor('!SmrTask', task_constructor)

load_configs()
load_objects()
list_context()

from smartrain.main import run
