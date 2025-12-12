from nagra import Statement
from nagra.utils import strip_lines
from nagra.schema import Schema
from nagra.table import Table


def test_create_table(empty_transaction):
    flavor = empty_transaction.flavor
    schema = Schema()
    Table(
        "my_table",
        columns={
            "name": "varchar",
            "score": "int",
        },
        natural_key=["name"],
        not_null=["score"],
        default={
            "score": "0",
        },
        primary_key="custom_id",
        schema=schema,
    )
    lines = list(schema.setup_statements(trn=empty_transaction))
    create_table, create_idx = map(strip_lines, lines)

    match flavor:
        case "postgresql":
            assert create_table == [
                'CREATE TABLE  "my_table" (',
                '"custom_id" BIGSERIAL PRIMARY KEY',
                ",",
                '"name" TEXT',
                "NOT NULL",
                ",",
                '"score" INTEGER',
                "NOT NULL",
                "DEFAULT 0",
                ");",
            ]
        case "sqlite":
            assert create_table == [
                'CREATE TABLE  "my_table" (',
                '"custom_id" INTEGER PRIMARY KEY',
                ",",
                '"name" TEXT',
                "NOT NULL",
                ",",
                '"score" INTEGER',
                "NOT NULL",
                "DEFAULT 0",
                ");",
            ]
        case "mssql":
            assert create_table == [
                "CREATE TABLE [my_table] (",
                "[custom_id] BIGINT IDENTITY(1,1) PRIMARY KEY",
                ",",
                "[name] NVARCHAR(200)",
                "NOT NULL",
                ",",
                "[score] INT",
                "NOT NULL",
                "DEFAULT 0",
                ");",
            ]
    match flavor:
        case "postgresql" | "sqlite":
            assert create_idx == [
                'CREATE UNIQUE INDEX my_table_idx ON "my_table" (',
                '"name"',
                ");",
            ]
        case "mssql":
            assert create_idx == [
                "CREATE UNIQUE INDEX [my_table_idx] ON [my_table] ([name]",
                ");",
            ]


def test_create_table_pk_is_fk(empty_transaction):
    flavor = empty_transaction.flavor
    schema = Schema()
    # Table with no natural key and a fk in the primary key
    # It should be created second
    Table(
        "score",
        columns={
            "concept": "bigint",
            "score": "int",
        },
        primary_key="concept",
        foreign_keys={
            "concept": "concept",
        },
        schema=schema,
    )
    Table(  # Regular table
        "concept",
        columns={
            "name": "varchar",
        },
        natural_key=["name"],
        primary_key="concept_id",
        schema=schema,
    )
    lines = list(schema.setup_statements(trn=empty_transaction))

    match flavor:
        case "postgresql":
            assert lines == [
                'CREATE TABLE  "concept" (\n   "concept_id" BIGSERIAL PRIMARY KEY\n'
                '    , \n\n   "name" TEXT\n    NOT NULL\n);',
                'CREATE TABLE  "score" (\n   "concept" BIGINT PRIMARY KEY\n'
                '    CONSTRAINT fk_concept REFERENCES "concept"("concept_id") ON DELETE CASCADE\n'
                '    , \n\n   "score" INTEGER\n);',
                'ALTER TABLE "score"\n ADD COLUMN "concept" BIGINT NOT NULL\n'
                ' CONSTRAINT fk_concept REFERENCES "concept"("concept_id") ON DELETE CASCADE;',
                'CREATE UNIQUE INDEX concept_idx ON "concept" (\n  "name"\n);',
            ]
        case "sqlite":
            assert lines == [
                'CREATE TABLE  "concept" (\n   "concept_id" INTEGER PRIMARY KEY\n'
                '    , \n\n   "name" TEXT\n    NOT NULL\n);',
                'CREATE TABLE  "score" (\n   "concept" INTEGER PRIMARY KEY\n'
                '    CONSTRAINT fk_concept REFERENCES "concept"("concept_id") ON DELETE CASCADE\n'
                '    , \n\n   "score" INTEGER\n);',
                'ALTER TABLE "score"\n ADD COLUMN "concept" INTEGER NOT NULL\n'
                ' CONSTRAINT fk_concept REFERENCES "concept"("concept_id") ON DELETE CASCADE;',
                'CREATE UNIQUE INDEX concept_idx ON "concept" (\n  "name"\n);',
            ]
        case "mssql":
            assert lines == [
                "CREATE TABLE [concept] (\n"
                "  [concept_id] BIGINT IDENTITY(1,1) PRIMARY KEY\n"
                ");",
                "CREATE TABLE [score] (\n"
                "  [concept] BIGINT PRIMARY KEY\n"
                "  , CONSTRAINT fk_concept FOREIGN KEY ([concept])\n"
                "    REFERENCES [concept] ([concept_id])\n"
                "    ON DELETE CASCADE\n"
                ");",
                "ALTER TABLE [concept]\n ADD [name] NVARCHAR(200) NOT NULL\n;\n",
                "ALTER TABLE [score]\n ADD [score] INT\n;\n",
                "CREATE UNIQUE INDEX [concept_idx] ON [concept] ([name]\n);",
            ]


def test_create_table_no_pk(empty_transaction):
    flavor = empty_transaction.flavor
    schema = Schema()
    Table(  # Regular table
        "concept",
        columns={
            "name": "varchar",
        },
        natural_key=["name"],
        schema=schema,
    )
    Table(  #  Table with no primary key and a fk in the nk
        "score",
        columns={
            "concept": "bigint",
            "score": "int",
        },
        natural_key=["concept"],
        primary_key=None,
        foreign_keys={
            "concept": "concept",
        },
        schema=schema,
    )
    lines = list(schema.setup_statements(trn=empty_transaction))
    (
        create_concept,
        create_score,
        add_score_concept_fk,
        create_concept_idx,
        create_score_idx,
    ) = map(strip_lines, lines)

    match flavor:
        case "postgresql":
            assert create_concept == [
                'CREATE TABLE  "concept" (',
                '"id" BIGSERIAL PRIMARY KEY',
                ",",
                '"name" TEXT',
                "NOT NULL",
                ");",
            ]
            assert add_score_concept_fk == [
                'ALTER TABLE "score"',
                'ADD COLUMN "concept" BIGINT NOT NULL',
                'CONSTRAINT fk_concept REFERENCES "concept"("id") ON DELETE CASCADE;',
            ]
        case "sqlite":
            assert create_concept == [
                'CREATE TABLE  "concept" (',
                '"id" INTEGER PRIMARY KEY',
                ",",
                '"name" TEXT',
                "NOT NULL",
                ");",
            ]
            assert add_score_concept_fk == [
                'ALTER TABLE "score"',
                'ADD COLUMN "concept" INTEGER NOT NULL',
                'CONSTRAINT fk_concept REFERENCES "concept"("id") ON DELETE CASCADE;',
            ]
        case "postgresql" | "sqlite":
            assert create_score == [
                'CREATE TABLE  "score" (',
                '"score" INTEGER',
                ");",
            ]
        case "mssql":
            assert create_concept == [
                "CREATE TABLE [concept] (",
                "[id] BIGINT IDENTITY(1,1) PRIMARY KEY",
                ",",
                "[name] NVARCHAR(200)",
                "NOT NULL",
                ");",
            ]
            assert create_score == [
                "CREATE TABLE [score] (",
                "[score] INT",
                ");",
            ]

    match flavor:
        case "postgresql" | "sqlite":
            assert create_concept_idx == [
                'CREATE UNIQUE INDEX concept_idx ON "concept" (',
                '"name"',
                ");",
            ]
            assert create_score_idx == [
                'CREATE UNIQUE INDEX score_idx ON "score" (',
                '"concept"',
                ");",
            ]
        case "mssql":
            assert create_concept_idx == [
                "CREATE UNIQUE INDEX [concept_idx] ON [concept] ([name]",
                ");",
            ]
            assert create_score_idx == [
                "CREATE UNIQUE INDEX [score_idx] ON [score] ([concept]",
                ");",
            ]


def test_create_unique_index():
    stmt = Statement("create_unique_index").table("my_table").natural_key(["name"])
    doc = stmt()
    lines = strip_lines(doc)
    assert lines == [
        'CREATE UNIQUE INDEX my_table_idx ON "my_table" (',
        '"name"',
        ");",
    ]
