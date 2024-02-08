from nagra import Transaction, Schema
from nagra.transaction import ExecMany


def test_resolver(person):
    with Transaction("postgresql:///nagra"):
        Schema.default.setup()

    values = ((i,) for i in range(5))
    with Transaction("postgresql:///nagra", rollback=True):
        rsv = ExecMany("SELECT %s", values)
        results = list(rsv)
        assert results == [(0,), (1,), (2,), (3,), (4,)]

    values = ((i,) for i in range(5))
    with Transaction("postgresql:///nagra", rollback=True):
        rsv = ExecMany("SELECT %s from person WHERE false", values)
        results = list(rsv)
        assert results == [None] * 5
