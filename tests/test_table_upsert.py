from nagra.utils import strip_lines


def test_simple_upsert_stm(person):
    upsert = person.upsert("name", "parent.name")
    assert list(upsert.resolve_stm) == ["parent"]
    res = list(strip_lines(upsert.resolve_stm["parent"]))
    assert res == [
        "SELECT",
        '"person"."id"',
        'FROM "person"',
        "WHERE",
        '"person"."name" = %s',
        ";",
    ]
    res = list(strip_lines(upsert.stm()))
    assert res == [
        'INSERT INTO "person" (name, parent)',
        "VALUES (",
        "%s,%s",
        ")",
        "ON CONFLICT (",
        "name",
        ")",
        "DO UPDATE SET",
        "parent = EXCLUDED.parent",
    ]


def test_simple_upsert(transaction, person):
    # First upsert
    upsert = person.upsert("name")
    upsert.execute("Big Bob")
    (rows,) = list(person.select("name").execute())
    assert rows == ("Big Bob",)

    # Second one
    upsert = person.upsert("name", "parent.name")
    upsert.execute("Bob", "Big Bob")
    rows = list(person.select("name", "parent.name").execute())
    assert rows == [("Big Bob", None), ("Bob", "Big Bob")]


def test_insert(transaction, person):
    # First upsert
    upsert = person.upsert("name")
    records = [("Big Bob",), ("Bob",)]
    upsert.executemany(records)

    # Second one
    upsert = person.insert("name", "parent.name")
    upsert.execute("Bob", "Big Bob")
    rows = list(person.select("name", "parent.name").execute())
    assert rows == [("Big Bob", None), ("Bob", None)]


def test_upsert_stmt_with_id(person):
    upsert = person.upsert("id", "name", "parent.name")
    res = list(strip_lines(upsert.stm()))
    assert res == [
        'INSERT INTO "person" (id, name, parent)',
        "VALUES (",
        "%s,%s,%s",
        ")",
        "ON CONFLICT (",
        "id",
        ")",
        "DO UPDATE SET",
        "name = EXCLUDED.name , parent = EXCLUDED.parent",
    ]


def test_upsert_exec_with_id(transaction, person):
    # Add parent
    upsert = person.upsert("id", "name")
    upsert.execute(1, "Big Bob")
    (rows,) = list(person.select("id", "name").execute())
    assert rows == (1, "Big Bob")

    # Add child
    upsert = person.upsert("id", "name", "parent.name")
    upsert.execute(2, "Bob", "Big Bob")
    rows = list(person.select("name", "parent.name").execute())
    assert rows == [("Big Bob", None), ("Bob", "Big Bob")]

    # Update child
    upsert = person.upsert("id", "name")
    upsert.execute(2, "BOB")
    cond = "(= id 2)"
    (rows,) = person.select("name").where(cond).execute()
    assert rows == ("BOB",)


def test_many_upsert(transaction, person):
    # First upsert
    upsert = person.upsert("name")
    records = [("Big Alice",), ("Big Bob",)]
    upsert.executemany(records)
    rows = list(person.select("name").execute())
    assert len(rows) == 2

    # Second upsert
    upsert = person.upsert("name", "parent.name")
    records = [
        (
            "Alice",
            "Big Alice",
        ),
        (
            "Bob",
            "Big Bob",
        ),
    ]
    upsert.executemany(records)

    rows = list(person.select("name", "parent.name").execute())
    assert len(rows) == 4

