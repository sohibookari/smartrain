import yaml
import smartrain.context as ctx

from smartrain.config.config import BasicConfig
from smartrain.builder import (CheckInSampleBuilder, TypicalSampleBuilder, TypicalTaskType, Processor, TimeProcessor)
from smartrain.cio import DictReader
from smartrain.mapper import Mapper


def basic_config_constructor(loader, node):
    fields = loader.construct_mapping(node)
    return BasicConfig(**fields)


yaml.add_constructor('!BasicConfig', basic_config_constructor)

with open('.\\default.yaml') as f:
    try:
        smrconfig = yaml.load(f, Loader=yaml.FullLoader)
        ctx.set('config', smrconfig)
    except yaml.YAMLError as e:
        print(e)

ctx.set('mapper', Mapper())
smrconfig.getLogger().info('smart-rain has been successful loaded.')

from smartrain.main import run_all_tasks
