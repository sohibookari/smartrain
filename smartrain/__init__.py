import os

import yaml
import smartrain.context as ctx

from smartrain.mapper import Mapper
from smartrain.constructor import (basic_config_constructor, task_config_constructor,
                                   task_constructor, resource_config_constructor)


def load_configs():
    yaml_files = [name for name in os.listdir('.\\') if name.endswith('.yaml') or name.endswith('.yml')]
    for file in yaml_files:
        with open('.\\' + file) as f:
            try:
                cfg = yaml.load(f, Loader=yaml.FullLoader)
                if isinstance(cfg, dict):
                    continue
                ctx.set(file.split('.')[0], 'config', cfg)
            except yaml.YAMLError as e:
                print(e)


def load_objects():
    ctx.set('mapper', 'config', Mapper())


def load_resources():
    ctx.get('logger').info('preloading resources, which may takes a few seconds.')
    # ctx.get('resources').fetch_all()


def list_context():
    ctx.get('logger').info('loaded context object: ' + [i for i in ctx.get_keys()].__str__())
    ctx.get('logger').info('smart-rain has been successful loaded.')


yaml.add_constructor('!BasicConfig', basic_config_constructor)
yaml.add_constructor('!TaskConfig', task_config_constructor)
yaml.add_constructor('!ResourceConfig', resource_config_constructor)
yaml.add_constructor('!SmrTask', task_constructor)

load_configs()
load_objects()
load_resources()
list_context()

from smartrain.main import run
