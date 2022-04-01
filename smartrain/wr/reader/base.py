import smartrain.context as ctx
from smartrain.constructor import BasicConfig


class Reader:
    def __init__(self):
        self.config: BasicConfig = ctx.get('config')
        self.logger = self.config.getLogger()
