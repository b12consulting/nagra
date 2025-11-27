from nagra.transaction import ExecMany


def test_resolver(transaction, person):
    placeholder = "?" if transaction.flavor in ("sqlite", "mssql") else "%s"
    values = [(i,) for i in range(5)]
    rsv = ExecMany(
        f"SELECT {placeholder}",
        values,
        trn=transaction,
    )
    results = list(rsv)
    assert results == [(0,), (1,), (2,), (3,), (4,)]

    false = "false" if transaction.flavor != "mssql" else "0=1"
    rsv = ExecMany(
        f"SELECT {placeholder} from person WHERE {false}",
        values,
        trn=transaction,
    )
    results = list(rsv)
    assert results == [None] * 5
