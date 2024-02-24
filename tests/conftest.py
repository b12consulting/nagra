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
    one2many = {
        "orgs": "org.person",
        "skills": "skill.person",
    }
)


org_table = Table(
    "org",
    columns={
        "name": "varchar",
        "person": "int",
    },
    foreign_keys={
        "person": "person",
    },
    natural_key=["name"],
    one2many = {
        "addresses": "address.org",
    }
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
    foreign_keys={
        "person": "person",
 }
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


@pytest.fixture(scope="session")
def person():
    person_table.delete()
    return person_table


@pytest.fixture(scope="session")
def org():
    org_table.delete()
    return org_table

@pytest.fixture(scope="session")
def address():
    address_table.delete()
    return address_table


@pytest.fixture(scope="session")
def kitchensink():
    kitchensink_table.delete()
    return kitchensink_table

@pytest.fixture(scope="session")
def temperature():
    temperature_table.delete()
    return temperature_table


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
