from nagra import Statement
from nagra.utils import strip_lines


def test_debug_statement():
    stmt = Statement("debug")
    doc = stmt()
    assert doc == ""

    stmt = stmt.foo("bar")
    doc = stmt()
    assert doc == "foo=bar\n"

    stmt = stmt.ham("spam")
    doc = stmt()
    assert doc == "foo=bar\nham=spam\n"


def test_mssql_select_limit():
    stmt = Statement(
        "select",
        flavor="mssql",
        table="people",
        columns=["[people].[name]"],
        joins=[],
        conditions=[],
        limit=5,
        offset=None,
        groupby=[],
        orderby=[],
        distinct_on=[],
        distinct=False,
    )
    lines = strip_lines(stmt())
    assert lines[0] == "SELECT TOP 5"
    assert lines[1] == "[people].[name]"
    assert lines[2] == "FROM [people]"


def test_mssql_select_offset_fetch():
    stmt = Statement(
        "select",
        flavor="mssql",
        table="people",
        columns=["[people].[name]"],
        joins=[],
        conditions=[],
        limit=10,
        offset=20,
        groupby=[],
        orderby=["[people].[name] ASC"],
        distinct_on=[],
        distinct=False,
    )
    doc = stmt()
    assert "ORDER BY" in doc
    assert "OFFSET 20 ROWS" in doc
    assert "FETCH NEXT 10 ROWS ONLY" in doc


def test_mssql_update_outputs_returning():
    stmt = Statement(
        "update",
        flavor="mssql",
        table="people",
        columns={"name": None, "id": None},
        condition_key=["id"],
        returning=["id"],
    )
    doc = stmt()
    assert "OUTPUT inserted.[id]" in doc


def test_mssql_upsert_uses_merge():
    stmt = Statement(
        "upsert",
        flavor="mssql",
        table="people",
        columns={"name": None, "email": None},
        conflict_key=["email"],
        do_update=False,
        returning=["id"],
    )
    doc = stmt()
    lines = strip_lines(doc)
    assert lines == [
        'MERGE INTO [people] AS target',
        'USING (',
        'SELECT? AS [name], ? AS [email]',
        ') AS source',
        'ON (',
        'target.[email] = source.[email]',
        ')',
        'WHEN NOT MATCHED THEN',
        'INSERT ([name], [email]',
        ')',
        'VALUES (source.[name], source.[email]',
        ')',
        'OUTPUT inserted.[id]',
        ';']


