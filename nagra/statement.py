from functools import partial

from nagra.utils import template
from nagra.transaction import dummy_transaction


class Statement:
    def __init__(self, template, flavor=None, **params):
        self._template = template
        self._params = params
        self._flavor = flavor or dummy_transaction.flavor

    def __call__(self):
        path = f"{self._flavor}/{self._template}.sql"
        return template(path).render(**self._params, _all_params=self._params)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise ValueError(f"Invalid statement parameter {name}")
        return partial(self._setter, name)

    def _setter(self, name, value):
        self._params[name] = value
        return self
