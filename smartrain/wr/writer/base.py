import os
import time

import smartrain.context as ctx
from smartrain.constructor import BasicConfig


class Writer:
    def __init__(self, file, path=None):
        self.config: BasicConfig = ctx.get('config')
        self.logger = self.config.getLogger()
        self.file = file
        self.path = path

    def abs_(self):
        if not self.path:
            self.path = self.config.OUTPUT_DIR + time.strftime("%Y-%m-%d", time.localtime()) + '\\'
        if not os.path.exists(self.path):
            os.mkdir(self.path)
        return self.path + self.file

    def exists(self) -> bool:
        return os.path.exists(self.abs_())

    def name(self) -> str:
        return self.file
