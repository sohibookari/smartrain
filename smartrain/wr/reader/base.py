import smartrain.context as ctx
from smartrain.config import BasicConfig


class Reader:
    def __init__(self):
        self.config: BasicConfig = ctx.get('config')
        self.logger = self.config.getLogger()
