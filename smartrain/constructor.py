import logging
import os
import sys
import time
from logging import Logger
from typing import Any

import yaml

import smartrain.context as ctx


class TaskConfig(yaml.YAMLObject):
    def __init__(self, tasks) -> None:
        self.tasks = tasks

    def __repr__(self) -> str:
        return super().__repr__() + '\n' + str(self.__dict__)


class SmrTask(yaml.YAMLObject):
    def __init__(self, name, type, loads, set=True) -> None:
        self.logger: Logger = ctx.get('config').getLogger()
        self.name = name
        self.type = type
        self.loads = loads
        self.set = set

    def _run_single(self, load) -> Any:
        return getattr(ctx.get('mapper'), '_do_%s' % self.type)(load)

    def run(self) -> None:
        result_dict = {}
        for load in self.loads:
            result_dict[load] = self._run_single(load)
        ctx.set(self.name, result_dict)

    def __repr__(self) -> str:
        return super().__repr__() + '\n' + str(self.__dict__)


class BasicConfig:
    def __init__(self, db_url, output_dir, cache_dir, log_dir, cache_file,
                 log_to_file, log_to_console, log_level, log_formatter):

        self._logger = None

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

    def getLogger(self) -> logging.Logger:
        return self._logger

    def _generate_logger(self):
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(self.LOG_LEVEL)

        formatter = logging.Formatter(self.LOG_FORMATTER)
        if self.LOG_TO_FILE:
            file_handler = logging.FileHandler(self.LOG_PATH)
            file_handler.setLevel(self.LOG_LEVEL)
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)

        if self.LOG_TO_CONSOLE:
            console_handler = logging.StreamHandler(stream=sys.stdout)
            console_handler.setLevel(self.LOG_LEVEL)
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)

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


def task_constructor(loader, node):
    fields = loader.construct_mapping(node)
    return SmrTask(**fields)
