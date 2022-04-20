import smartrain.context as ctx


class Reader:
    def __init__(self):
        self.config = ctx.get('config')
        self.logger = ctx.get('logger')
