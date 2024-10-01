from nagra.utils import strip_lines


def test_delete(person):
    # Simple delete
    delete = person.delete()
    stm = delete.stm()
    res = strip_lines(stm)
    assert res == ['DELETE FROM "person"']

    # With a condition
    delete = person.delete('(= name "spam")')
    stm = delete.stm()
    res = strip_lines(stm)
    assert res == ['DELETE FROM "person"', "WHERE", '"person"."name" = \'spam\'']

    # With a join
    delete = person.delete('(= parent.name "spam")')
    stm = delete.stm()
    res = strip_lines(stm)
    assert res == [
        'DELETE FROM "person"',
        'WHERE "person".id IN (',
        'SELECT "person".id from "person"',
        'LEFT JOIN "person" as parent_0 ON (',
        'parent_0."id" = "person"."parent"',
        ")WHERE",
        '"parent_0"."name" = \'spam\'',
        ")",
    ]


def test_delete_cascade(transaction, person, skill):
    # Insert persons
    person.upsert("name").executemany(
        [
            ("Yankee",),
            ("Zulu",),
        ]
    )

    # Add skills (person is a fk and is not null)
    skill.upsert("name", "person.name").executemany(
        [
            ("Cooking", "Yankee"),
            ("Fishing", "Zulu"),
        ]
    )
    # Check created skills
    assert sorted(skill) == [("Cooking", "Yankee"), ("Fishing", "Zulu")]

    # Delete person and list skills
    person.delete('(= name "Zulu")').execute()
    assert list(skill) == [("Cooking", "Yankee")]

    # Same, but with an  arg
    person.delete("(= name {})").execute("Yankee")
    assert list(skill) == []
