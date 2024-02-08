import pytest

from nagra import Table, Transaction, Schema

person_table = Table(
    "person",
    columns={
        "name": "varchar",
        "parent": "int",
    },
    foreign_keys={
        "parent": "person",
    },
    natural_key=["name"],
)


kitchensink_table = Table(
    "kitchensink",
    columns={
        "varchar": "varchar",
        "bigint": "bigint",
        "float": "float",
        "int": "int",
        "timestamp": "timestamp",
        "bool": "bool",
    },
    natural_key=["varchar"],
)


@pytest.fixture(scope="session")
def person():
    return person_table

@pytest.fixture(scope="session")
def kitchensink():
    return kitchensink_table


DSN= [
    "postgresql:///nagra",
    "sqlite://",
    # "duckdb://",
]
@pytest.fixture(scope="function", params=DSN)
def transaction(request):
    dsn = request.param
    with Transaction(dsn, rollback=True) as tr:
        Schema.default.setup()
        yield tr
