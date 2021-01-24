import functools

from blizz import _inspect


def deduplication(func):
    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(func)
        assert relation is not None
        res = func(*args, **kwargs)
        # _deduplication(r=relation, data=res) todo: implement this
        return res

    return _decorated


def fill_defaults(func):
    @functools.wraps(func)
    def _decorated(*args, **kwargs):
        relation = _inspect.get_class_that_defined_method(func)
        assert relation is not None
        res = func(*args, **kwargs)
        # _fill_defaults(r=relation, data=res) todo: implement this
        return res

    return _decorated
