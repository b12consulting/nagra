from argparse import Namespace

import pytest

from nagra.cli import select


class FakeSelect:
    def __init__(self, columns):
        self.columns = columns
        self.alias_values = None

    def aliases(self, *aliases):
        self.alias_values = aliases
        return self

    def where(self, *conditions):
        return self

    def limit(self, value):
        return self

    def orderby(self, *orders):
        return self

    def execute(self, *args):
        return [(1, 2)]

    def dtypes(self, *aliases):
        names = aliases or self.columns
        return [(name, str) for name in names]


class FakeTable:
    def default_columns(self, skip_pk, skip_blob):
        return ["name", "city"]

    def select(self, *columns):
        return FakeSelect(columns)


class FakeSchema:
    def get(self, name):
        return FakeTable()


def cli_args(**kwargs):
    defaults = dict(
        table="person",
        columns=["name", "city"],
        alias=[],
        where=[],
        limit=None,
        orderby=None,
        pivot=False,
        table_fmt=None,
    )
    defaults.update(kwargs)
    return Namespace(**defaults)


def test_select_cli_applies_full_and_partial_aliases(monkeypatch):
    printed = []
    monkeypatch.setattr("nagra.cli.print_table", lambda rows, headers, *args, **kwargs: printed.append(headers))

    select(cli_args(alias=[["person_name", "person_city"]]), FakeSchema())
    assert printed[-1] == ["person_name", "person_city"]

    select(cli_args(alias=[["person_name"]]), FakeSchema())
    assert printed[-1] == ["person_name", "city"]


def test_select_cli_rejects_too_many_aliases():
    with pytest.raises(ValueError, match="More aliases"):
        select(cli_args(alias=[["one", "two", "three"]]), FakeSchema())
