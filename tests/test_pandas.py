import zoneinfo
from datetime import datetime, date
from uuid import UUID
from pandas import concat, DataFrame, to_datetime

from nagra import Transaction


def test_to_pandas(transaction, temperature):
    # Upsert
    temperature.upsert("timestamp", "city", "value").executemany(
        [
            ("1970-01-02", "Berlin", 10),
            ("1970-01-02", "London", 12),
        ]
    )
    # Read data
    df = temperature.select().to_pandas()
    assert list(df.columns) == ["timestamp", "city", "value"]
    assert sorted(df.city) == ["Berlin", "London"]

    # Read data - with chunks
    dfs = temperature.select().to_pandas(chunked=1)
    df = concat(list(dfs))
    assert list(df.columns) == ["timestamp", "city", "value"]
    assert sorted(df.city) == ["Berlin", "London"]

    # Read with custom arg
    cond = "(= value {})"
    df = temperature.select().where(cond).to_pandas(12)
    assert list(df.columns) == ["timestamp", "city", "value"]
    assert sorted(df.city) == ["London"]


def test_from_pandas(transaction, kitchensink):
    df = DataFrame(
        {
            "varchar": ["ham"],
            "bigint": [1],
            "float": [1.0],
            "int": [1],
            "timestamp": to_datetime(["1970-01-01 00:00:00"]),
            "timestamptz": to_datetime(["1970-01-01 00:00:00+00:00"]),
            "bool": [True],
            "date": ["1970-01-01"],
            "json": [{}],
            "uuid": ["F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11C"],
            "max": ["max"],
            "(var true)": ["true"],
        }
    )

    # UPSERT
    kitchensink.upsert().from_pandas(df)
    (row,) = kitchensink.select()
    BRUTZ = zoneinfo.ZoneInfo(key='Europe/Brussels')
    if Transaction.current.flavor == "postgresql":
        assert row == (
            "ham",
            1,
            1.0,
            1,
            datetime(1970, 1, 1, 0, 0),
            datetime(1970, 1, 1, 1, 0, tzinfo=BRUTZ),
            True,
            date(1970, 1, 1),
            {},
            UUID("F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11C"),
            "max",
            "true",
        )
    else:
        assert row == (
            "ham",
            1,
            1.0,
            1,
            "1970-01-01",
            "1970-01-01 00:00:00+00:00",
            1,
            "1970-01-01",
            "{}",
            "F1172BD3-0A1D-422E-8ED6-8DC2D0F8C11C",
            "max",
            "true",
        )

    # SELECT
    new_df = kitchensink.select().to_pandas()
    pass
