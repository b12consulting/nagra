import pytest
from typeguard import install_import_hook

install_import_hook("nagra")

from nagra import Table, Transaction, Schema, View

DSN = [
    "postgresql:///nagra",
    "sqlite://",
    # "mssql://sa:p4ssw0rD@127.0.0.1/nagra?trust_server_certificate=yes",
    # "postgresql://yugabyte:yugabyte@localhost:5433/nagra"
    # "duckdb://",
]


@pytest.fixture(scope="session", params=DSN)
def dsn(request):
    return request.param


@pytest.fixture(scope="session")
def schema(dsn):
    schema = Schema()

    person_table = Table(
        "person",
        columns={
            "name": "varchar",
            "parent": "bigint",
        },
        foreign_keys={
            "parent": "person",
        },
        natural_key=["name"],
        one2many={
            "orgs": "org.person",
            "skills": "skill.person",
            "grand_parent": "person.parent.parent",
        },
        schema=schema,
    )

    org_table = Table(
        "org",
        columns={
            "name": "varchar",
            "person": "bigint",
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
        schema=schema,
    )

    address_table = Table(
        "address",
        columns={
            "city": "varchar",
            "org": "bigint",
            "country": "int",
        },
        foreign_keys={
            "org": "org",
        },
        natural_key=["city"],
        schema=schema,
    )

    country_table = Table(
        "country",
        columns={
            "name": "varchar",
        },
        natural_key=["name"],
        one2many={
            "populations": "population.country",
        },
        schema=schema,
    )

    skill_table = Table(
        "skill",
        columns={
            "name": "varchar",
            "person": "bigint",
        },
        natural_key=["name"],
        not_null=["person"],
        foreign_keys={
            "person": "person",
        },
        schema=schema,
    )

    kitchensink_table = Table(
        "kitchensink",
        columns={
            "varchar": "varchar",
            "bigint": "bigint",
            "float": "float",
            "int": "int",
            "timestamp": "timestamp",
            "timestamptz": "timestamptz",
            "bool": "bool",
            "date": "date",
            "json": "json",
            "uuid": "uuid",
            "max": "varchar",
            "true": "varchar",
            "blob": "blob",
        },
        natural_key=["varchar"],
        not_null=["int"],
        schema=schema,
    )

    temperature_table = Table(
        "temperature",
        columns={
            "timestamp": "timestamp",
            "city": "varchar",
            "value": "float",
        },
        natural_key=["timestamp", "city"],
        schema=schema,
    )

    if "postgresql" in dsn:
        temperature_no_nk_pk_table = Table(
            "temperature_no_nk_pk",
            columns={
                "timestamp": "timestamp",
                "city": "varchar",
                "value": "float",
            },
            primary_key=None,
            schema=schema,
        )

    parameter_table = Table(
        "parameter",
        columns={
            "name": "str",
            "timestamps": "timestamp []",
            "values": "float []",
        },
        natural_key=["name"],
        schema=schema,
    )

    population_table = Table(
        "population",
        columns={
            "country": "bigint",
            "year": "int",
            "value": "int",
        },
        natural_key=["country", "year"],
        foreign_keys={
            "country": "country",
        },
        schema=schema,
    )

    max_pop_view = View(
        "max_pop",
        columns={
            "country": "varchar",
            "max": "float",
        },
        as_select="""
        SELECT name as country, max(population.value) as max
        FROM country
        JOIN population on (population.country = country.id)
        GROUP BY country.name
        """,
        schema=schema,
    )

    min_pop_view = View(
        "min_pop",
        view_columns={
            "country": "name",
            "min": "(min populations.value)",
        },
        view_select="country",
        schema=schema,
    )

    # A table without natural keys
    value_table = Table(
        "value",
        columns={
            "value": "float",
        },
        schema=schema,
    )

    return schema


@pytest.fixture(scope="session")
def person(schema: Schema):
    return schema.tables["person"]


@pytest.fixture(scope="session")
def org(schema: Schema):
    return schema.tables["org"]


@pytest.fixture(scope="session")
def skill(schema: Schema):
    return schema.tables["skill"]


@pytest.fixture(scope="session")
def address(schema: Schema):
    return schema.tables["address"]


@pytest.fixture(scope="session")
def kitchensink(schema: Schema):
    return schema.tables["kitchensink"]


@pytest.fixture(scope="session")
def temperature(schema: Schema):
    return schema.tables["temperature"]


@pytest.fixture(scope="session")
def temperature_no_nk_pk(schema: Schema):
    return schema.tables.get("temperature_no_nk_pk", None)


@pytest.fixture(scope="session")
def country(schema: Schema):
    return schema.tables["country"]


@pytest.fixture(scope="session")
def population(schema: Schema):
    return schema.tables["population"]


@pytest.fixture(scope="session")
def min_pop(schema: Schema):
    return schema.views["min_pop"]


@pytest.fixture(scope="session")
def max_pop(schema: Schema):
    return schema.views["max_pop"]


@pytest.fixture(scope="session")
def parameter(schema: Schema):
    return schema.tables["parameter"]


@pytest.fixture(scope="session")
def value(schema: Schema):
    return schema.tables["value"]


@pytest.fixture(scope="function")
def empty_transaction(schema, dsn):
    with Transaction(dsn, rollback=True) as tr:
        yield tr


@pytest.fixture(scope="function")
def transaction(request, schema, empty_transaction):
    # Start from empty_transaction and load tables
    schema.create_tables(empty_transaction)
    yield empty_transaction


@pytest.fixture(scope="function", params=(True, False))
def cacheable_transaction(request, schema, dsn):
    fk_cache = request.param
    with Transaction(dsn, rollback=True, fk_cache=fk_cache) as tr:
        schema.create_tables(tr)
        yield tr
