from nagra import Statement
from nagra.utils import strip_lines


def test_create_table():
    stmt = (
        Statement("create_table")
        .table("my_table")
        .columns(
            {
                "name": "varchar",
                "value": "integer",
            }
        )
        .natural_key(["name"])
        .not_null(["name"])
    )

    doc = stmt()
    lines = list(strip_lines(doc))
    assert lines == ['CREATE TABLE  "my_table" (', "id BIGSERIAL PRIMARY KEY", ");"]


def test_create_unique_index():
    stmt = Statement("create_unique_index").table("my_table").natural_key(["name"])
    doc = stmt()
    lines = list(strip_lines(doc))
    assert lines == [
        'CREATE UNIQUE INDEX IF NOT EXISTS my_table_idx ON "my_table" (',
        '"name"',
        ");",
    ]
