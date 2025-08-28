import datetime
import pytest

from nagra import Transaction
from nagra.utils import strip_lines


def test_simple_select(person):
    stm = person.select("name").stm()
    res = " ".join(strip_lines(stm))
    assert res == 'SELECT "person"."name" FROM "person" ;'


def test_select_with_join(person):
    stm = person.select("name", "parent.parent.name").stm()
    res = strip_lines(stm)
    assert res == [
        "SELECT",
        '"person"."name", "parent_1"."name"',
        'FROM "person"',
        'LEFT JOIN "person" as parent_0 ON (',
        'parent_0."id" = "person"."parent"',
        ")",
        'LEFT JOIN "person" as parent_1 ON (',
        'parent_1."id" = "parent_0"."parent"',
        ")",
        ";",
    ]


def test_kitchensink_select(kitchensink):
    stm = kitchensink.select().stm()
    res = strip_lines(stm)
    assert res == [
        "SELECT",
        '"kitchensink"."varchar", "kitchensink"."bigint", "kitchensink"."float", '
        '"kitchensink"."int", "kitchensink"."timestamp", "kitchensink"."timestamptz", '
        '"kitchensink"."bool", "kitchensink"."date", "kitchensink"."json", '
        '"kitchensink"."uuid", "kitchensink"."max", "kitchensink"."true", '
        '"kitchensink"."blob"',
        'FROM "kitchensink"',
        ";",
    ]

    stm = kitchensink.select(".true").stm()
    res = strip_lines(stm)
    assert res == ["SELECT", '"kitchensink"."true"', 'FROM "kitchensink"', ";"]

    stm = kitchensink.select("true").stm()
    res = strip_lines(stm)
    assert res == ["SELECT", "true", 'FROM "kitchensink"', ";"]


def test_select_clone(person):
    queries = [
        person.select("name")
        .limit(1)
        .offset(1)
        .where("(= name 'spam')")
        .groupby("name"),
        person.select("name")
        .offset(1)
        .where("(= name 'spam')")
        .groupby("name")
        .limit(1),
        person.select("name")
        .where("(= name 'spam')")
        .groupby("name")
        .limit(1)
        .offset(1),
        person.select("name")
        .groupby("name")
        .limit(1)
        .offset(1)
        .where("(= name 'spam')"),
    ]

    expected = queries[0].stm()
    for q in queries[1:]:
        assert q.stm() == expected


def test_select_with_where(person):
    select = person.select("name").where("(= id {})")
    stm = select.stm()
    res = strip_lines(stm)
    assert res == [
        "SELECT",
        '"person"."name"',
        'FROM "person"',
        "WHERE",
        '"person"."id" = %s',
        ";",
    ]

    stm = select.where("(= name 'spam')").stm()
    res = strip_lines(stm)
    assert res == [
        "SELECT",
        '"person"."name"',
        'FROM "person"',
        "WHERE",
        '"person"."id" = %s AND "person"."name" = \'spam\'',
        ";",
    ]


def test_select_where_and_join(person):
    select = person.select("name").where("(= parent.name 'foo')")
    stm = select.stm()
    res = strip_lines(stm)
    assert res == [
        "SELECT",
        '"person"."name"',
        'FROM "person"',
        'LEFT JOIN "person" as parent_0 ON (',
        'parent_0."id" = "person"."parent"',
        ")",
        "WHERE",
        '"parent_0"."name" = \'foo\'',
        ";",
    ]


@pytest.mark.parametrize("op", ["min", "max", "sum"])
def test_simple_agg(person, op):
    # MIN
    stm = person.select(f"({op} name)").stm()
    res = " ".join(strip_lines(stm))
    assert res == f'SELECT {op}("person"."name") FROM "person" ;'


def test_count(person):
    stm = person.select("(count *)").stm()
    res = " ".join(strip_lines(stm))
    assert res == 'SELECT count(*) FROM "person" ;'

    stm = person.select("(count 1)").stm()
    res = " ".join(strip_lines(stm))
    assert res == 'SELECT count(1) FROM "person" ;'


def test_groupby(person):
    # Explicit
    stm = person.select("name", "(count *)").groupby("name").stm()
    res = " ".join(strip_lines(stm))
    assert (
        res
        == 'SELECT "person"."name", count(*) FROM "person" GROUP BY "person"."name" ;'
    )

    # implicit
    stm = person.select("name", "(count *)").stm()
    res = " ".join(strip_lines(stm))
    assert (
        res
        == 'SELECT "person"."name", count(*) FROM "person" GROUP BY "person"."name" ;'
    )


def test_orderby(person):
    # asc
    stm = person.select("name").orderby("name").stm()
    res = " ".join(strip_lines(stm))
    assert res == 'SELECT "person"."name" FROM "person" ORDER BY "person"."name" asc ;'

    # desc
    stm = person.select("name").orderby(("name", "desc")).stm()
    res = " ".join(strip_lines(stm))
    assert res == 'SELECT "person"."name" FROM "person" ORDER BY "person"."name" desc ;'

    # with join
    stm = person.select("name").orderby("parent.name").stm()
    res = " ".join(strip_lines(stm))
    assert res == (
        'SELECT "person"."name" FROM "person" LEFT JOIN "person" as parent_0 ON ( '
        'parent_0."id" = "person"."parent" ) ORDER BY "parent_0"."name" asc ;'
    )


def test_o2m_stm(person, org):
    # Combine one2many and implicit joins
    select = person.select(
        "name",
        "orgs.name",
        "parent.name",
        "parent.parent.name",
    )
    stm = select.stm()
    res = strip_lines(stm)
    expected = [
        "SELECT",
        '"person"."name", "orgs_0"."name", "parent_1"."name", ' '"parent_2"."name"',
        'FROM "person"',
        'LEFT JOIN "org" as orgs_0 ON (',
        'orgs_0."person" = "person"."id"',
        ")",
        'LEFT JOIN "person" as parent_1 ON (',
        'parent_1."id" = "person"."parent"',
        ")",
        'LEFT JOIN "person" as parent_2 ON (',
        'parent_2."id" = "parent_1"."parent"',
        ")",
        ";",
    ]
    assert res == expected

    # Multiple one2many
    select = person.select(
        "name",
        "orgs.country",
        "skills.name",
    )
    stm = select.stm()
    res = strip_lines(stm)
    expected = [
        "SELECT",
        '"person"."name", "orgs_0"."country", "skills_1"."name"',
        'FROM "person"',
        'LEFT JOIN "org" as orgs_0 ON (',
        'orgs_0."person" = "person"."id"',
        ")",
        'LEFT JOIN "skill" as skills_1 ON (',
        'skills_1."person" = "person"."id"',
        ")",
        ";",
    ]
    assert res == expected

    # Use a on2many after a many2one
    select = org.select(
        "name",
        "person.name",
        "person.skills.name",
    )
    stm = select.stm()
    res = strip_lines(stm)
    expected = [
        "SELECT",
        '"org"."name", "person_0"."name", "skills_1"."name"',
        'FROM "org"',
        'LEFT JOIN "person" as person_0 ON (',
        'person_0."id" = "org"."person"',
        ")",
        'LEFT JOIN "skill" as skills_1 ON (',
        'skills_1."person" = "person_0"."id"',
        ")",
        ";",
    ]
    assert res == expected

    # Use a on2many after a one2many
    select = person.select("name", "orgs.addresses.city")
    stm = select.stm()
    res = strip_lines(stm)
    expected = [
        "SELECT",
        '"person"."name", "addresses_1"."city"',
        'FROM "person"',
        'LEFT JOIN "org" as orgs_0 ON (',
        'orgs_0."person" = "person"."id"',
        ")",
        'LEFT JOIN "address" as addresses_1 ON (',
        'addresses_1."org" = "orgs_0"."id"',
        ")",
        ";",
    ]
    assert res == expected


def test_o2m_select(transaction, person, org, address):
    # Test with actual data
    person.upsert("name").execute("Charly")
    org.upsert("name", "person.name").execute("Alpha", "Charly")
    org.upsert("name", "person.name").execute("Beta", "Charly")
    address.upsert("city", "org.name").executemany(
        [
            ("Ankara", "Alpha"),
            ("Athens", "Alpha"),
            ("Beirut", "Beta"),
        ]
    )
    rows = list(
        person.select("name", "orgs.addresses.city").orderby("orgs.addresses.city")
    )
    assert rows == [("Charly", "Ankara"), ("Charly", "Athens"), ("Charly", "Beirut")]


def test_agg(transaction, temperature):
    temperature.upsert("timestamp", "city", "value").executemany(
        [
            ("1970-01-01", "Berlin", 10),
            ("1970-01-01", "London", 12),
        ]
    )
    rows = list(
        temperature.select(
            "city",
        )
    )
    assert len(rows) == 2

    # String concat
    is_pg = Transaction.current().flavor == "postgresql"
    if is_pg:
        select = temperature.select("(string_agg city ',')")
    else:
        select = temperature.select("(group_concat city)")
    rows = list(select)
    assert len(rows) == 1
    (record,) = rows
    assert record[0] in ("Berlin,London", "London,Berlin")

    # Strings into array
    if is_pg:
        (record,) = list(temperature.select("(array_agg city)"))
        assert sorted(record[0]) == ["Berlin", "London"]

    # sum, avg, min and max
    for op, expected in [("sum", 22), ("min", 10), ("max", 12), ("avg", 11)]:
        select = temperature.select(f"({op} value)")
        (record,) = list(select)
        assert expected == record[0]

    # Add more rows
    temperature.upsert("timestamp", "city", "value").executemany(
        [
            ("1970-01-02", "Berlin", 10),
            ("1970-01-02", "London", 12),
        ]
    )

    records = dict(temperature.select("city", "(sum value)").groupby("city"))
    assert records == {"Berlin": 20.0, "London": 24.0}

    # Json agg
    if is_pg:
        select = temperature.select("(json_object_agg city value)")
        (record,) = list(select)
        assert record[0] == {"Berlin": 10, "London": 12}


def test_date_op(transaction, temperature):
    is_pg = Transaction.current().flavor == "postgresql"

    temperature.upsert("timestamp", "city", "value").executemany(
        [
            ("1970-01-02", "Berlin", 10),
            ("1970-01-02", "London", 12),
        ]
    )
    if is_pg:
        select = temperature.select("(extract 'year' timestamp)")
        records = list(select)
        assert records[0][0] == 1970
    else:
        select = temperature.select("(strftime '%Y' timestamp)")
        records = list(select)
        assert records[0][0] == "1970"
    assert len(records) == 2


def test_to_dict(transaction, temperature):
    # Upsert
    temperature.upsert("timestamp", "city", "value").executemany(
        [
            ("1970-01-02", "Berlin", 10),
            ("1970-01-02", "London", 12),
        ]
    )
    # Read data
    expected_date = datetime.datetime(1970, 1, 2, 0, 0)
    if transaction.flavor == "sqlite":
        expected_date = str(expected_date.date())

    records = list(temperature.select().orderby("city").to_dict())
    assert len(records) == 2
    assert records[0] == {
        "timestamp": expected_date,
        "city": "Berlin",
        "value": 10.0,
    }
    # Read with custom arg
    cond = "(= value {})"
    (record,) = temperature.select().where(cond).to_dict(12)
    assert record == {
        "timestamp": expected_date,
        "city": "London",
        "value": 12.0,
    }

@pytest.mark.parametrize("nest_with_param", [False, True])
def test_to_nested_dict(transaction, person, nest_with_param):
    # Upsert
    person.upsert("name", "parent.name").execute("Alice", None)
    person.upsert("name", "parent.name").execute("Bob", "Alice")
    person.upsert("name", "parent.name").execute("Charly", "Bob")

    # Read data
    select = person.select("name", "parent.name", "parent.parent.name")
    if nest_with_param:
        records = list(select.to_dict(nest=True))
    else:
        records = list(select.to_nested_dict())

    assert records == [
        {
            "name": "Alice",
            "parent": None,
        },
        {
            "name": "Bob",
            "parent": {
                "name": "Alice",
                "parent": None,
            },
        },
        {
            "name": "Charly",
            "parent": {
                "name": "Bob",
                "parent": {
                    "name": "Alice",
                },
            },
        },
    ]
