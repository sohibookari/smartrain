__context_dict = {}


def set(name, value):
    __context_dict[name] = value


def get(name):
    try:
        return __context_dict[name]
    except KeyError:
        raise ContextUndefinedError


class ContextUndefinedError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

    def __str__(self) -> str:
        return super().__str__()
