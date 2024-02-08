from nagra import Statement


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
