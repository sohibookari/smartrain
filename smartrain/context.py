__context_dict = {}


def set(name, domain, value):
    key = "%s@%s" % (name, domain)
    __context_dict[key] = value


def _get(name):
    try:
        return __context_dict[name]
    except KeyError:
        raise ContextUndefinedError(name)


def get(name, domain='config'):
    return _get('%s@%s' % (name, domain))


def fetch(name, domain=None):
    try:
        return get(name, domain)
    except ContextUndefinedError:
        return get('resources', 'config').fetch(domain, name)


def parse(str, source=None):
    if str.find('@') != -1:
        # domain of load specified.
        t_load, t_source = str.split('@')
    else:
        t_load, t_source = str, source
    return t_load, t_source


def get_keys():
    return list(__context_dict.keys())


class ContextUndefinedError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

    def __str__(self) -> str:
        return super().__str__()
