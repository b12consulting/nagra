from nagra.utils import strip_lines


def test_delete(person):
    delete = person.delete()
    stm = delete.stm()
    res = list(strip_lines(stm))
    assert res == ['DELETE FROM "person"']

    delete = person.delete('(= name "spam")')
    stm = delete.stm()
    res = list(strip_lines(stm))
    assert res == ['DELETE FROM "person"', "WHERE", '"person"."name" = \'spam\'']

    delete = person.delete('(= parent.name "spam")')
    stm = delete.stm()
    res = list(strip_lines(stm))
    assert res == [
        'DELETE FROM "person"',
        'WHERE "person".id IN (',
        'SELECT "person".id from "person"',
        'LEFT JOIN "person" as parent_0 ON (',
        'parent_0."id" = "person"."parent"',
        ')WHERE',
        '"parent_0"."name" = \'spam\'',
        ')']

