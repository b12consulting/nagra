import pytest

from typeguard import install_import_hook

install_import_hook("nagra")

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
    one2many={
        "orgs": "org.person",
        "skills": "skill.person",
    },
)


org_table = Table(
    "org",
    columns={
        "name": "varchar",
        "person": "int",
        "status": "varchar",
    },
    foreign_keys={
        "person": "person",
    },
    natural_key=["name"],
    one2many={
        "addresses": "address.org",
    },
    default={
        "status": "'OK'",
    },
)


address_table = Table(
    "address",
    columns={
        "city": "varchar",
        "org": "int",
        "country": "int",
    },
    foreign_keys={
        "org": "org",
    },
    natural_key=["city"],
)


country_table = Table(
    "country",
    columns={
        "name": "varchar",
    },
    natural_key=["name"],
)


skill_table = Table(
    "skill",
    columns={
        "name": "varchar",
        "person": "int",
    },
    natural_key=["name"],
    not_null=["person"],
    foreign_keys={
        "person": "person",
    },
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
        "date": "date",
        "json": "json",
        "uuid": "uuid",
    },
    natural_key=["varchar"],
)

temperature_table = Table(
    "temperature",
    columns={
        "timestamp": "timestamp",
        "city": "varchar",
        "value": "float",
    },
    natural_key=["timestamp", "city"],
)

parameter_table = Table(
    "parameter",
    columns={
        "name": "str",
        "timestamps": "timestamp []",
        "values": "float []",
    },
    natural_key=["name"],
)


@pytest.fixture(scope="session")
def person():
    return person_table


@pytest.fixture(scope="session")
def org():
    return org_table


@pytest.fixture(scope="session")
def skill():
    return skill_table


@pytest.fixture(scope="session")
def address():
    return address_table


@pytest.fixture(scope="session")
def kitchensink():
    return kitchensink_table


@pytest.fixture(scope="session")
def temperature():
    return temperature_table

@pytest.fixture(scope="session")
def parameter():
    return parameter_table


DSN = [
    "postgresql:///nagra",
    "sqlite://",
    # "postgresql://yugabyte:yugabyte@localhost:5433/nagra"
    # "duckdb://",
]


@pytest.fixture(scope="function", params=DSN)
def empty_transaction(request):
    dsn = request.param
    with Transaction(dsn, rollback=True) as tr:
        yield tr


@pytest.fixture(scope="function")
def transaction(request, empty_transaction):
    # Start from empty_transaction and load tables
    Schema.default.create_tables(empty_transaction)
    yield empty_transaction
