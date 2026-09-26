"""Owned, recursively immutable JSON mappings for public context evidence."""
from collections.abc import Mapping

class FrozenDict(dict):
    def _readonly(self, *args, **kwargs):
        raise TypeError("context evidence is immutable")
    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = __ior__ = _readonly
    def __deepcopy__(self, memo):
        return self

def freeze(value):
    if isinstance(value, Mapping):
        return FrozenDict({k: freeze(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(freeze(v) for v in value)
    return value
