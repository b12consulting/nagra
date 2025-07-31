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
            "true": ["true"],
            "blob": [b"blob"],
        }
    )

    # UPSERT
    kitchensink.upsert().from_pandas(df)
    (row,) = kitchensink.select()
    BRUTZ = zoneinfo.ZoneInfo(key="Europe/Brussels")
    if Transaction.current().flavor == "postgresql":
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
            b"blob",
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
            "blob",
        )

    # SELECT with operator
    if transaction.flavor == "postgresql":
        new_df = kitchensink.select(
            "(date_bin '5 days' timestamptz '1900-01-01')",
        ).to_pandas()
        new_df.columns = ["ts"]
        assert str(new_df.ts.dtype) == 'datetime64[ns, Europe/Brussels]'
        ts = new_df.ts[0]
        assert ts.isoformat() == '1969-12-30T01:00:00+01:00'
        # NOTE the above result expected:
        # ```
        # =# SELECT date_bin('5 days', TIMESTAMPTZ '1970-01-01 00:00:00+00', '1900-01-01');
        #       date_bin
        # ------------------------
        #  1969-12-30 01:00:00+01
        # (1 row)
        # ```

    if transaction.flavor == "sqlite":
        new_df = kitchensink.select(
            "(+ float float)",
            "(- int int)",
        ).to_pandas()
        new_df.columns = ["float", "int"]
        assert str(new_df.float.dtype) == 'float64'
        assert str(new_df.int.dtype) == 'int64'
