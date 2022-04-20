import logging
import os
import sys
import time
from logging import Logger
from typing import Any

import yaml
import smartrain.context as ctx
from smartrain.wr import DictReader


class TaskConfig(yaml.YAMLObject):
    def __init__(self, tasks) -> None:
        self.tasks = tasks

    def __repr__(self) -> str:
        return super().__repr__() + '\n' + str(self.__dict__)


class SmrTask(yaml.YAMLObject):
    def __init__(self, name, type, loads, source=None, domain=None, split=True) -> None:
        self.logger: Logger = ctx.get('logger')
        self.name = name
        self.domain = domain
        self.type = type
        self.split = split
        self.source = source
        self.loads = loads

    def _run_single(self, data, params=None):
        if params is None:
            params = []
        return getattr(ctx.get('mapper'), '_do_%s' % self.type)(data, *params)

    def _run_split(self, t_load, t_source) -> Any:
        load = ctx.fetch(t_load, t_source)
        return self._run_single(load, [t_load])

    def run(self) -> None:
        self.logger.info('Task %s is executing.' % self.name)
        start_t = time.time()
        if self.split:
            for index, load in enumerate(self.loads):
                # load is an id of the real resource.
                t_load, t_source = ctx.parse(load, self.source if self.source else self.domain)
                self.logger.info('Load %s is on process. (%d/%d)' % (t_load, index+1, len(self.loads)))
                res = self._run_split(t_load, t_source)
                if res is not None:
                    ctx.set(t_load, self.domain if self.domain else self.name, res)
        else:
            data_set = []
            for load in self.loads:
                t_load, t_source = ctx.parse(load, self.source)
                data_set.append((t_load, ctx.fetch(t_load, t_source)))
            res = self._run_single(data_set)
            if res is not None:
                ctx.set(self.name, self.domain if self.domain else self.name, res)
        end_t = time.time()
        self.logger.debug(ctx.list())
        self.logger.info('Task %s ended in %.4f' % (self.name, end_t-start_t))

    def __repr__(self) -> str:
        return super().__repr__() + '\n' + str(self.__dict__)


class ResourcesConfig:
    def __init__(self, resources):
        self.resources = resources

    def fetch_all(self):
        for res in self.resources.items():
            data = DictReader(res[1]).read_threading()
            for d in data:
                ctx.set(d[0], res[0], d[1])

    def fetch(self, res, target):
        return DictReader(self.resources[res]).read_view(target)

    def __repr__(self) -> str:
        return super().__repr__() + '\n' + str(self.__dict__)


class BasicConfig:
    def __init__(self, db_url, output_dir, cache_dir, log_dir, cache_file,
                 log_to_file, log_to_console, log_level, log_formatter):

        self.PROJECT_DIR = os.path.dirname(os.path.abspath(os.path.join(__file__, "")))
        self.OUTPUT_DIR = output_dir.replace('$PWD', self.PROJECT_DIR)
        self.CACHE_DIR = cache_dir.replace('$PWD', self.PROJECT_DIR)
        self.LOG_DIR = log_dir.replace('$PWD', self.PROJECT_DIR)
        self.CACHE_FILE = cache_file

        self.LOG_TO_FILE = log_to_file
        self.LOG_TO_CONSOLE = log_to_console
        self.LOG_PATH = self.LOG_DIR + "smartrain_%s.log" % time.strftime("%Y_%m_%d", time.localtime())
        self.LOG_LEVEL = getattr(logging, log_level)
        self.LOG_FORMATTER = log_formatter

        self.DB_URL = db_url

        self._create_workdir()
        self._generate_logger()

    def __repr__(self) -> str:
        return super().__repr__() + '\n' + str(self.__dict__)

    def _generate_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(self.LOG_LEVEL)

        formatter = logging.Formatter(self.LOG_FORMATTER)
        if self.LOG_TO_FILE:
            file_handler = logging.FileHandler(self.LOG_PATH)
            file_handler.setLevel(self.LOG_LEVEL)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        if self.LOG_TO_CONSOLE:
            console_handler = logging.StreamHandler(stream=sys.stdout)
            console_handler.setLevel(self.LOG_LEVEL)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        ctx.set('logger', 'config', logger)

    def _create_workdir(self):
        try:
            if not os.path.exists(self.LOG_DIR):
                os.mkdir(self.LOG_DIR)
            if not os.path.exists(self.OUTPUT_DIR):
                os.mkdir(self.OUTPUT_DIR)
            if not os.path.exists(self.CACHE_DIR):
                os.mkdir(self.CACHE_DIR)
        except OSError as e:
            print('An OSError occurred while creating the necessary folder, which may cause the job to fail.')
            print(e)


def basic_config_constructor(loader, node):
    fields = loader.construct_mapping(node)
    return BasicConfig(**fields)


def task_config_constructor(loader, node):
    fields = loader.construct_mapping(node)
    return TaskConfig(**fields)


def resource_config_constructor(loader, node):
    fields = loader.construct_mapping(node)
    return ResourcesConfig(**fields)


def task_constructor(loader, node):
    fields = loader.construct_mapping(node)
    return SmrTask(**fields)
