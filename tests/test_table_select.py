import pytest

from nagra.utils import strip_lines


def test_simple_select(person):
    stm = person.select("name").stm()
    res = " ".join(strip_lines(stm))
    assert res == 'SELECT "person"."name" FROM "person" ;'


def test_select_with_join(person):
    stm = person.select("name", "parent.parent.name").stm()
    res = list(strip_lines(stm))
    assert res == [
        "SELECT",
        '"person"."name", "parent_1"."name"',
        'FROM "person"',
        'LEFT JOIN "person" as parent_0 ON (parent_0.id = "person"."parent")',
        'LEFT JOIN "person" as parent_1 ON (parent_1.id = "parent_0"."parent")',
        ";",
    ]


def test_select_with_where(person):
    select = person.select("name", where="(= id {})")
    stm = select.stm()
    res = list(strip_lines(stm))
    assert res == [
        "SELECT",
        '"person"."name"',
        'FROM "person"',
        "WHERE",
        '"person"."id" = %s',
        ";",
    ]

    stm = select.where("(= name 'spam')").stm()
    res = list(strip_lines(stm))
    assert res == [
        "SELECT",
        '"person"."name"',
        'FROM "person"',
        "WHERE",
        '"person"."id" = %s AND "person"."name" = \'spam\'',
        ";",
    ]


def test_select_where_and_join(person):
    select = person.select("name", where="(= parent.name 'foo')")
    stm = select.stm()
    res = list(strip_lines(stm))
    assert res == [
        "SELECT",
        '"person"."name"',
        'FROM "person"',
        'LEFT JOIN "person" as parent_0 ON (parent_0.id = "person"."parent")',
        "WHERE",
        '"parent_0"."name" = \'foo\'',
        ";",
    ]


def test_suggest(transaction, person):
    # upsert
    upsert = person.upsert("name")
    records = [("Big Alice",), ("Big Bob",)]
    upsert.executemany(records)
    rows = list(person.select("name").execute())
    assert len(rows) == 2

    res = list(person.suggest("parent.name"))
    assert res == ["Big Alice", "Big Bob"]

    res = list(person.suggest("parent.name", like="%A%"))
    assert res == ["Big Alice"]


@pytest.mark.parametrize("op", ["min", "max", "sum"])
def test_simple_agg(person, op):
    # MIN
    stm = person.select(f"({op} name)").stm()
    res = " ".join(strip_lines(stm))
    assert res == f'SELECT {op}("person"."name") FROM "person" ;'


def test_count(person):
    stm = person.select("(count)").stm()
    res = " ".join(strip_lines(stm))
    assert res == 'SELECT count(*) FROM "person" ;'

    stm = person.select("(count 1)").stm()
    res = " ".join(strip_lines(stm))
    assert res == 'SELECT count(1) FROM "person" ;'


def test_groupby(person):
    # Explicit
    stm = person.select("name", "(count)").groupby("name").stm()
    res = " ".join(strip_lines(stm))
    assert (
        res
        == 'SELECT "person"."name", count(*) FROM "person" GROUP BY "person"."name" ;'
    )

    # implicit
    stm = person.select("name", "(count)").stm()
    res = " ".join(strip_lines(stm))
    assert (
        res
        == 'SELECT "person"."name", count(*) FROM "person" GROUP BY "person"."name" ;'
    )


def test_orderby(person):
    # asc
    stm = person.select("name").orderby("name").stm()
    res = " ".join(strip_lines(stm))
    assert (
        res
        == 'SELECT "person"."name" FROM "person" ORDER BY "person"."name" asc ;'
    )

    # desc
    stm = person.select("name").orderby(("name", "desc")).stm()
    res = " ".join(strip_lines(stm))
    assert (
        res
        == 'SELECT "person"."name" FROM "person" ORDER BY "person"."name" desc ;'
    )


    # with join
    stm = person.select("name").orderby("parent.name").stm()
    res = " ".join(strip_lines(stm))
    assert res == (
        'SELECT "person"."name" FROM "person" '
        'LEFT JOIN "person" as parent_0 ON (parent_0.id = "person"."parent") '
        'ORDER BY "parent_0"."name" asc ;'
    )
