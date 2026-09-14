# Nagra - Complete Documentation

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Core Concepts](#core-concepts)
5. [Table Definition](#table-definition)
6. [Schema Management](#schema-management)
7. [Database Operations](#database-operations)
8. [Query Builder](#query-builder)
9. [Transactions](#transactions)
10. [Views](#views)
11. [Command Line Interface](#command-line-interface)
12. [Integration Examples](#integration-examples)
13. [Data Types](#data-types)
14. [Best Practices](#best-practices)
15. [API Reference](#api-reference)
16. [Migration Guide](#migration-guide)
17. [Troubleshooting](#troubleshooting)

## Overview

Nagra is a Python ORM (Object-Relational Mapping) library designed specifically for OLAP (Online Analytical Processing) use cases. It emphasizes the declarative nature of relational databases and provides built-in features usually not available in traditional ORMs, such as:

- **Declarative table definitions** with natural keys and foreign key relationships
- **Built-in support for TOML schema definitions**
- **Automatic SQL generation** with join optimization
- **Command-line interface** for database operations
- **Native support for pandas DataFrames**
- **Views and aggregation queries**
- **Multi-database support** (SQLite, PostgreSQL, DuckDB)

### Key Features

- **Natural Key Focus**: Tables are designed around natural keys rather than artificial primary keys
- **Automatic Join Resolution**: Foreign key relationships are automatically resolved in queries
- **S-Expression Query Language**: Powerful query expressions using Polish notation
- **Schema Introspection**: Load existing database schemas automatically
- **Transaction Management**: Atomic operations with rollback support
- **CLI Tools**: Query and manage databases from the command line
- **Framework Integration**: Easy integration with FastAPI, Streamlit, and other frameworks

## Installation

### Basic Installation

```bash
pip install nagra
```

### PostgreSQL Support

```bash
pip install "nagra[pg]"
```

### Development Installation

```bash
pip install "nagra[dev]"
```

### Optional Dependencies

- `psycopg`: PostgreSQL adapter
- `pandas`: DataFrame support
- `pytest`: Testing framework

## Quick Start

### 1. Define Your Schema

**Python API:**

```python
from nagra import Table, Schema, Transaction

# Define tables using Python
city = Table(
    "city",
    columns={
        "name": "varchar",
        "lat": "varchar",
        "long": "varchar",
    },
    natural_key=["name"],
    one2many={
        "temperatures": "temperature.city",
    }
)

temperature = Table(
    "temperature",
    columns={
        "timestamp": "timestamp",
        "city": "int",
        "value": "float",
    },
    natural_key=["city", "timestamp"],
    foreign_keys={
        "city": "city",
    }
)
```

**TOML Schema:**

```toml
[city]
natural_key = ["name"]
[city.columns]
name = "varchar"
lat = "varchar"
long = "varchar"
[city.one2many]
temperatures = "temperature.city"

[temperature]
natural_key = ["city", "timestamp"]
[temperature.columns]
city = "bigint"
timestamp = "timestamp"
value = "float"
[temperature.foreign_keys]
city = "city"
```

### 2. Create Database and Insert Data

```python
from nagra import Schema, Transaction

# Load schema from TOML
schema_toml = """
[city]
natural_key = ["name"]
[city.columns]
name = "varchar"
lat = "varchar"
long = "varchar"
[city.one2many]
temperatures = "temperature.city"

[temperature]
natural_key = ["city", "timestamp"]
[temperature.columns]
city = "bigint"
timestamp = "timestamp"
value = "float"
[temperature.foreign_keys]
city = "city"
"""

with Transaction("sqlite://weather.db"):
    schema = Schema.from_toml(schema_toml)
    schema.create_tables()

    # Insert cities
    city = schema.get("city")
    cities = [
        ("Brussels", "50.8476° N", "4.3572° E"),
        ("London", "51.5074° N", "0.1278° W"),
    ]
    city.upsert("name", "lat", "long").executemany(cities)

    # Insert temperatures
    temperature = schema.get("temperature")
    temps = [
        ("Brussels", "2023-11-27T16:00", 6.5),
        ("Brussels", "2023-11-27T17:00", 7.0),
        ("London", "2023-11-27T16:00", 8.2),
    ]
    temperature.upsert("city.name", "timestamp", "value").executemany(temps)
```

### 3. Query Data

```python
# Simple select
records = list(city.select("name", "lat"))
print(records)
# [('Brussels', '50.8476° N'), ('London', '51.5074° N')]

# Query with joins (automatic)
temps = list(temperature.select("city.name", "timestamp", "value"))
print(temps)
# [('Brussels', '2023-11-27T16:00', 6.5), ...]

# Aggregation
avg_temps = temperature.select(
    "city.name",
    "(avg value)"
).groupby("city.name")
print(list(avg_temps))
# [('Brussels', 6.75), ('London', 8.2)]

# Filtering
brussels_temps = temperature.select(
    "timestamp",
    "value"
).where("(= city.name 'Brussels')")
print(list(brussels_temps))
```

## Core Concepts

### Natural Keys

Nagra is built around the concept of **natural keys** - meaningful identifiers that come from your business domain rather than artificial surrogate keys. Every table must define a natural key, which can be:

- **Single column**: `natural_key=["email"]`
- **Composite**: `natural_key=["country", "year"]`

Natural keys are used for:

- Unique constraints
- Upsert operations
- Foreign key relationships

### Foreign Key Relationships

Foreign keys in Nagra automatically resolve to natural keys of referenced tables:

```python
# This foreign key...
foreign_keys={"city": "city"}

# ...automatically references the natural key of the city table
# (not an artificial ID column)
```

### One-to-Many Relationships

Define reverse relationships for easier querying:

```python
# In the parent table
one2many={"temperatures": "temperature.city"}

# Now you can query:
city.select("name", "temperatures.value")
```

### Automatic Joins

Nagra automatically generates JOIN clauses when you reference foreign key columns:

```python
# This query...
temperature.select("city.name", "value")

# ...automatically generates:
# SELECT city_0.name, temperature.value
# FROM temperature
# LEFT JOIN city as city_0 ON (city_0.id = temperature.city)
```

## Table Definition

### Python API

```python
from nagra import Table

table = Table(
    name="user",
    columns={
        "email": "varchar",
        "first_name": "varchar",
        "last_name": "varchar",
        "birth_date": "date",
        "department": "int",
    },
    natural_key=["email"],
    foreign_keys={"department": "department"},
    not_null=["first_name", "last_name"],
    one2many={"projects": "project.user"},
    default={"active": "true"},
    primary_key="id",  # Default, can be None
    schema=Schema.default
)
```

### TOML Schema

```toml
[user]
natural_key = ["email"]
not_null = ["first_name", "last_name"]
[user.columns]
email = "varchar"
first_name = "varchar"
last_name = "varchar"
birth_date = "date"
department = "int"
[user.foreign_keys]
department = "department"
[user.one2many]
projects = "project.user"
[user.default]
active = "true"
```

### Column Parameters

- **columns**: Dictionary of column names to data types
- **natural_key**: List of columns forming the natural key
- **foreign_keys**: Dictionary mapping column names to referenced table names
- **not_null**: List of columns that cannot be NULL
- **one2many**: Dictionary defining reverse foreign key relationships
- **default**: Dictionary of default values for columns
- **primary_key**: Name of primary key column (default: "id", can be None)

### Tables Without Primary Keys

For large tables like time series data, you can disable primary keys:

```python
Table(
    "timeseries",
    columns={"timestamp": "timestamp", "value": "float"},
    natural_key=["timestamp"],
    primary_key=None  # No primary key
)
```

```toml
[timeseries]
natural_key = ["timestamp"]
primary_key = ""
[timeseries.columns]
timestamp = "timestamp"
value = "float"
```

## Schema Management

### Creating Schemas

```python
from nagra import Schema

# Empty schema
schema = Schema()

# From TOML file
schema = Schema.from_toml("schema.toml")

# From TOML string
schema = Schema.from_toml(toml_string)

# From file handle
with open("schema.toml") as f:
    schema = Schema.from_toml(f)
```

### Schema Operations

```python
# Add tables to schema
table = Table("users", columns={"name": "varchar"}, natural_key=["name"])
schema.tables["users"] = table

# Get table from schema
users_table = schema.get("users")

# Create all tables in database
with Transaction("sqlite://db.sqlite"):
    schema.create_tables()

# Load schema from existing database
with Transaction("postgresql://user:pass@host/db"):
    schema = Schema.from_db()
```

### Default Schema

Nagra maintains a default schema accessible globally:

```python
from nagra import Schema, Table

# Tables are added to default schema by default
Table("users", columns={"name": "varchar"}, natural_key=["name"])

# Access default schema
schema = Schema.default

# Reset default schema
Schema.default.reset()
```

## Database Operations

### Upsert (Insert or Update)

Upsert operations insert new records or update existing ones based on natural keys:

```python
# Single record
city.upsert("name", "lat", "long").execute(
    "Brussels", "50.8476° N", "4.3572° E"
)

# Multiple records
cities = [
    ("Brussels", "50.8476° N", "4.3572° E"),
    ("London", "51.5074° N", "0.1278° W"),
]
city.upsert("name", "lat", "long").executemany(cities)

# Using foreign key values
temperature.upsert("city.name", "timestamp", "value").execute(
    "Brussels", "2023-11-27T16:00", 6.5
)

# From pandas DataFrame
df = pd.DataFrame({
    'name': ['Paris', 'Berlin'],
    'lat': ['48.8566° N', '52.5200° N'],
    'long': ['2.3522° E', '13.4050° E']
})
city.upsert().from_pandas(df)

# From dictionary list
records = [
    {"name": "Madrid", "lat": "40.4168° N", "long": "3.7038° W"},
    {"name": "Rome", "lat": "41.9028° N", "long": "12.4964° E"}
]
city.upsert().from_dict(records)
```

### Select Queries

```python
# All columns (with automatic foreign key resolution)
records = list(temperature.select())

# Specific columns
records = list(temperature.select("timestamp", "value"))

# With foreign key columns
records = list(temperature.select("city.name", "value"))

# With expressions
records = list(temperature.select("(+ value 10)", "city.name"))

# Aggregation
avg_by_city = temperature.select(
    "city.name",
    "(avg value)"
).groupby("city.name")

# Filtering
filtered = temperature.select().where(
    "(> value 5)",
    "(= city.name 'Brussels')"
)

# Ordering
ordered = temperature.select().orderby("timestamp", "value")

# Limiting
limited = temperature.select().limit(10)

# Chaining
result = temperature.select("city.name", "value")\
    .where("(> value 5)")\
    .orderby("value desc")\
    .limit(5)
```

### Update Operations

```python
# Simple update
temperature.update(value=10).where("(= city.name 'Brussels')").execute()

# Multiple columns
temperature.update(
    value=15,
    timestamp="2023-11-27T18:00"
).where("(= id 1)").execute()

# With expressions
temperature.update(value="(+ value 1)").execute()
```

### Delete Operations

```python
# Delete with condition
temperature.delete().where("(< value 0)").execute()

# Delete all records (use with caution!)
temperature.delete().execute()
```

### Copy Operations

Bulk insert from files:

```python
# From CSV file
city.copy_from("cities.csv", columns=["name", "lat", "long"])

# With custom delimiter
city.copy_from("cities.txt", delimiter="\t")
```

## Query Builder

### S-Expression Query Language

Nagra uses S-expressions (Polish notation) for query expressions:

```python
# Equality
"(= city.name 'Brussels')"

# Comparison
"(> value 10)"
"(< timestamp '2023-01-01')"
"(<= value 100)"

# Logical operations
"(and (> value 5) (< value 15))"
"(or (= city.name 'Brussels') (= city.name 'London'))"
"(not (= status 'inactive'))"

# Arithmetic
"(+ value 10)"
"(- value tax)"
"(* value rate)"
"(/ total count)"

# String operations
"(length name)"
"(upper name)"
"(lower email)"

# Aggregation
"(avg value)"
"(sum amount)"
"(count *)"
"(min timestamp)"
"(max value)"

# Functions
"(coalesce value 0)"
```

### Advanced Queries

```python
# Subqueries using one2many relationships
city.select(
    "name",
    "(avg temperatures.value)",
    "(count temperatures.id)"
).groupby("name")

# Complex filtering
temperature.select().where(
    "(and "
    "  (> value 5) "
    "  (or "
    "    (= city.name 'Brussels') "
    "    (= city.name 'London')"
    "  )"
    ")"
)

# Window functions (if supported by database)
temperature.select(
    "timestamp",
    "value",
    "(lag value 1)"
).orderby("timestamp")
```

### Data Export

```python
# To pandas DataFrame
df = temperature.select().to_pandas()

# To dictionary list
records = temperature.select().to_dict()

# To dataclass
DataClass = temperature.select().to_dataclass()
```

## Transactions

### Basic Usage

```python
from nagra import Transaction

# Database connection
with Transaction("sqlite://data.db"):
    # All operations are atomic
    city.upsert("name").execute("Brussels")
    temperature.upsert("city.name", "value").execute("Brussels", 15.5)
    # Automatically committed on success, rolled back on exception
```

### Database URLs

```python
# SQLite
with Transaction("sqlite://data.db"):
    pass

# In-memory SQLite
with Transaction("sqlite://"):
    pass

# PostgreSQL
with Transaction("postgresql://user:password@host:5432/database"):
    pass

# Environment variables
import os
os.environ["NAGRA_DB"] = "postgresql://user:pass@host/db"
with Transaction():  # Uses environment variable
    pass
```

### Transaction Configuration

```python
# With foreign key caching for performance
with Transaction("sqlite://data.db", fk_cache=True):
    # Foreign key lookups are cached for faster upserts
    pass

# Access current transaction
from nagra import Transaction
current = Transaction.current()
```

### Nested Transactions

```python
with Transaction("sqlite://data.db"):
    city.upsert("name").execute("Brussels")

    try:
        with Transaction():  # Nested transaction
            temperature.upsert("city.name", "value").execute("Brussels", 15.5)
            # This will be rolled back if an error occurs
            raise Exception("Something went wrong")
    except Exception:
        # Outer transaction continues
        pass

    # This will still be committed
    city.upsert("name").execute("London")
```

## Views

Views provide a way to create virtual tables based on queries. They can be defined using either SQL or Nagra's S-expression syntax.

### SQL Views

```toml
[max_temperature]
as_select = """
SELECT city.name as city, max(temperature.value) as max_temp
FROM city
JOIN temperature on (temperature.city = city.id)
GROUP BY city.name
"""
[max_temperature.columns]
city = "str"
max_temp = "float"
```

### S-Expression Views

```toml
[avg_temperature]
view_select = "temperature"
[avg_temperature.view_columns]
avg_temp = "(avg value)"
city = "city.name"
[avg_temperature.foreign_keys]
city = "city"
```

### Using Views

```python
from nagra import View

# Access view like a table
max_temps = View.get("max_temperature").select()

# Query views
brussels_max = View.get("max_temperature").select("max_temp").where(
    "(= city 'Brussels')"
)

# Views support same operations as tables
df = View.get("avg_temperature").select().to_pandas()
```

### Python View Definition

```python
from nagra import View

view = View(
    "avg_temperature",
    view_columns={
        "avg_temp": "(avg value)",
        "city": "city.name"
    },
    view_select="temperature",
    foreign_keys={"city": "city"}
)
```

## Command Line Interface

Nagra provides a powerful CLI for database operations.

### Environment Setup

```bash
export NAGRA_DB="sqlite://data.db"
export NAGRA_SCHEMA="schema.toml"
```

### Schema Operations

```bash
# List all tables
nagra schema

# Show table details
nagra schema users orders

# Generate D2 diagram
nagra schema --d2 > schema.d2
```

### Data Queries

```bash
# Select all data from table
nagra select users

# Select specific columns
nagra select users name email

# With filtering
nagra select users name email --where "(> age 18)"

# With ordering
nagra select users --orderby name

# With limit
nagra select users --limit 10

# Output as CSV
nagra select users --csv > users.csv

# Pivot output (one key-value per record)
nagra select users --pivot
```

### Data Manipulation

```bash
# Delete records
nagra delete users --where "(= status 'inactive')"
```

### Advanced CLI Usage

```bash
# Override database
nagra -d "postgresql://user:pass@host/db" select users

# Override schema
nagra -s "prod_schema.toml" select users

# Combine options
nagra -d "sqlite://prod.db" select orders \
  --where "(> total 1000)" \
  --orderby "total desc" \
  --limit 50 \
  --csv
```

## Integration Examples

### FastAPI Integration

```python
from typing import List
from fastapi import FastAPI
from nagra import Transaction, Schema

app = FastAPI()
schema = Schema.from_toml("schema.toml")
DB = "sqlite://data.db"

@app.get("/cities", response_model=List[dict])
def get_cities():
    with Transaction(DB):
        return list(schema.get("city").select().to_dict())

@app.get("/temperatures/{city_name}")
def get_temperatures(city_name: str):
    with Transaction(DB):
        select = schema.get("temperature").select(
            "timestamp", "value"
        ).where("(= city.name {})")
        return list(select.execute(city_name))

# Auto-generate endpoints
def create_endpoints():
    with Transaction(DB):
        schema.create_tables()

    for name, table in schema.tables.items():
        select = table.select()
        dataclass = select.to_dataclass()

        @app.get(f"/{name}", response_model=List[dataclass])
        def get_data(table=table):
            with Transaction(DB):
                return list(table.select().to_dict())

create_endpoints()
```

### Streamlit Integration

```python
import streamlit as st
from nagra import Transaction, Schema
import pandas as pd

# Initialize
DB = "sqlite://data.db"
schema = Schema.from_toml("schema.toml")

@st.cache_data
def load_cities():
    with Transaction(DB):
        return [city for city, in schema.get("city").select("name")]

@st.cache_data
def load_temperatures(city_name):
    with Transaction(DB):
        select = schema.get("temperature").select(
            "timestamp", "value"
        ).where("(= city.name {})")
        rows = select.execute(city_name)
        return pd.DataFrame(rows, columns=["timestamp", "value"])

# UI
st.title("Weather Dashboard")

city = st.selectbox("Select City", load_cities())
if city:
    df = load_temperatures(city)
    st.line_chart(df.set_index("timestamp"))
    st.dataframe(df)
```

### Jupyter Notebook Integration

```python
# Cell 1: Setup
from nagra import Transaction, Schema
import pandas as pd
import matplotlib.pyplot as plt

schema = Schema.from_toml("schema.toml")
db = "sqlite://analysis.db"

# Cell 2: Data Loading
with Transaction(db):
    df = schema.get("temperature").select(
        "city.name", "timestamp", "value"
    ).to_pandas()

# Cell 3: Analysis
avg_temps = df.groupby("city.name")["value"].mean()
avg_temps.plot(kind="bar")
plt.title("Average Temperature by City")
plt.show()

# Cell 4: Advanced Query
with Transaction(db):
    daily_avg = schema.get("temperature").select(
        "city.name",
        "(date timestamp)",
        "(avg value)"
    ).groupby("city.name", "(date timestamp)")

    daily_df = daily_avg.to_pandas()
```

### Data Pipeline Integration

```python
import pandas as pd
from nagra import Transaction, Schema

def extract_data():
    """Extract data from source"""
    return pd.read_csv("source_data.csv")

def transform_data(df):
    """Transform and clean data"""
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.dropna()
    return df

def load_data(df):
    """Load data into Nagra tables"""
    schema = Schema.from_toml("schema.toml")

    with Transaction("postgresql://user:pass@host/warehouse"):
        # Ensure tables exist
        schema.create_tables()

        # Load cities
        cities = df[["city", "lat", "long"]].drop_duplicates()
        schema.get("city").upsert().from_pandas(cities)

        # Load measurements
        measurements = df[["city", "timestamp", "temperature"]]
        schema.get("temperature").upsert().from_pandas(measurements)

# ETL Pipeline
def run_pipeline():
    df = extract_data()
    df = transform_data(df)
    load_data(df)

if __name__ == "__main__":
    run_pipeline()
```

## Data Types

Nagra supports a comprehensive set of data types that map to appropriate database-specific types.

### Basic Types

| Nagra Type    | Python Type | PostgreSQL    | SQLite    | Description             |
| ------------- | ----------- | ------------- | --------- | ----------------------- |
| `varchar`     | `str`       | `TEXT`        | `TEXT`    | Variable-length string  |
| `text`        | `str`       | `TEXT`        | `TEXT`    | Large text              |
| `int`         | `int`       | `INTEGER`     | `INTEGER` | 32-bit integer          |
| `bigint`      | `int`       | `BIGINT`      | `INTEGER` | 64-bit integer          |
| `float`       | `float`     | `REAL`        | `REAL`    | Floating point number   |
| `bool`        | `bool`      | `BOOLEAN`     | `INTEGER` | Boolean value           |
| `date`        | `date`      | `DATE`        | `TEXT`    | Date only               |
| `timestamp`   | `datetime`  | `TIMESTAMP`   | `TEXT`    | Date and time           |
| `timestamptz` | `datetime`  | `TIMESTAMPTZ` | `TEXT`    | Timestamp with timezone |
| `json`        | `dict/list` | `JSON`        | `TEXT`    | JSON data               |
| `uuid`        | `str`       | `UUID`        | `TEXT`    | UUID string             |
| `blob`        | `bytes`     | `BYTEA`       | `BLOB`    | Binary data             |

### Array Types (PostgreSQL)

PostgreSQL supports array types, which are represented in SQLite as JSON:

```python
Table(
    "measurements",
    columns={
        "name": "varchar",
        "values": "float []",  # Array of floats
        "timestamps": "timestamp []",  # Array of timestamps
    },
    natural_key=["name"]
)
```

```toml
[measurements]
natural_key = ["name"]
[measurements.columns]
name = "varchar"
values = "float []"
timestamps = "timestamp []"
```

### Type Conversion

Nagra automatically handles type conversion between Python and database types:

```python
# Dates and timestamps
from datetime import date, datetime

temperature.upsert("timestamp", "date_only").execute(
    datetime.now(),
    date.today()
)

# JSON data
metadata = {"source": "sensor_1", "calibrated": True}
sensor.upsert("name", "metadata").execute("temp_sensor", metadata)

# Arrays (PostgreSQL)
values = [1.5, 2.3, 4.1, 3.7]
measurement.upsert("name", "values").execute("experiment_1", values)
```

### Custom Type Mapping

You can extend type support by modifying the type mapping:

```python
from nagra.table import _TYPE_ALIAS

# Add custom type
_TYPE_ALIAS["decimal"] = "decimal"
```

## Best Practices

### Schema Design

1. **Use Natural Keys**: Design tables around meaningful business identifiers

   ```python
   # Good: Natural key
   Table("users", columns={"email": "varchar"}, natural_key=["email"])

   # Avoid: Meaningless surrogate keys only
   ```

2. **Define Relationships**: Use foreign keys and one2many for clear relationships

   ```python
   Table("orders",
         foreign_keys={"customer": "customers"},
         one2many={"items": "order_items.order"})
   ```

3. **Use Not Null Constraints**: Explicitly define required fields
   ```python
   Table("users", not_null=["email", "name"], ...)
   ```

### Performance Optimization

1. **Use Transactions**: Group related operations in transactions

   ```python
   with Transaction(db):
       # Multiple operations here are atomic and faster
       for batch in batches:
           table.upsert().executemany(batch)
   ```

2. **Enable Foreign Key Caching**: For heavy upsert workloads

   ```python
   with Transaction(db, fk_cache=True):
       # Foreign key lookups are cached
       table.upsert("foreign_key.name", "value").executemany(data)
   ```

3. **Use Bulk Operations**: Prefer `executemany` over multiple `execute` calls

   ```python
   # Good
   table.upsert().executemany(records)

   # Slow
   for record in records:
       table.upsert().execute(*record)
   ```

### Query Optimization

1. **Select Only Needed Columns**: Don't use `select()` without parameters if you only need specific columns

   ```python
   # Good
   users.select("email", "name")

   # Potentially slow if table has many columns
   users.select()
   ```

2. **Use Appropriate Filters**: Filter as early as possible

   ```python
   # Good: Filter before aggregation
   sales.select("(sum amount)").where("(> date '2023-01-01')").groupby("region")
   ```

3. **Leverage Indexes**: Natural keys automatically get unique indexes

### Error Handling

1. **Handle Schema Errors**: Catch and handle schema definition errors

   ```python
   from nagra.exceptions import IncorrectSchema

   try:
       Table("users", columns={"id": "int"}, natural_key=["email"])  # Error: email not in columns
   except IncorrectSchema as e:
       print(f"Schema error: {e}")
   ```

2. **Database Connection Errors**: Handle connection failures gracefully
   ```python
   try:
       with Transaction("postgresql://user:pass@host/db"):
           # Database operations
           pass
   except Exception as e:
       print(f"Database error: {e}")
   ```

### Code Organization

1. **Separate Schema Definitions**: Keep schema definitions in separate files

   ```
   project/
   ├── schema.toml
   ├── models.py
   └── main.py
   ```

2. **Use Schema Modules**: Organize related tables together

   ```python
   # models/users.py
   from nagra import Table, Schema

   user_schema = Schema()

   Table("users", schema=user_schema, ...)
   Table("profiles", schema=user_schema, ...)
   ```

3. **Environment Configuration**: Use environment variables for database URLs

   ```python
   import os

   DB_URL = os.environ.get("DATABASE_URL", "sqlite://default.db")
   SCHEMA_FILE = os.environ.get("SCHEMA_FILE", "schema.toml")
   ```

## API Reference

### Table Class

#### Constructor

```python
Table(
    name: str,
    columns: dict,
    natural_key: Optional[list[str]] = None,
    foreign_keys: Optional[dict] = None,
    not_null: Optional[list[str]] = None,
    one2many: Optional[dict] = None,
    default: Optional[dict] = None,
    primary_key: Optional[str] = "id",
    schema: Schema = Schema.default,
    is_view: Optional[bool] = False,
)
```

#### Methods

**select(\*columns) → Select**

```python
# Select all columns with automatic joins
users.select()

# Select specific columns
users.select("name", "email")

# Select with foreign key columns
orders.select("customer.name", "total")
```

**upsert(\*columns) → Upsert**

```python
# Upsert specific columns
users.upsert("email", "name")

# Upsert all columns
users.upsert()
```

**update(**kwargs) → Update\*\*

```python
# Update with values
users.update(name="John", email="john@example.com")

# Update with expressions
products.update(price="(* price 1.1)")
```

**delete() → Delete**

```python
# Delete all records
users.delete()

# Delete with conditions (add .where())
users.delete().where("(= status 'inactive')")
```

**copy_from(file_path, \*\*kwargs)**

```python
# Copy from CSV
users.copy_from("users.csv")

# With custom options
users.copy_from("data.txt", delimiter="\t", columns=["name", "email"])
```

#### Class Methods

**get(name, schema=Schema.default) → Table**

```python
# Get table from default schema
users = Table.get("users")

# Get table from specific schema
users = Table.get("users", schema=my_schema)
```

### Schema Class

#### Constructor

```python
Schema(tables=None, views=None)
```

#### Methods

**from_toml(source) → Schema**

```python
# From file path
schema = Schema.from_toml("schema.toml")

# From string
schema = Schema.from_toml(toml_string)

# From file handle
with open("schema.toml") as f:
    schema = Schema.from_toml(f)
```

**from_db(tables=None, trn=None) → Schema**

```python
# Load all tables from database
schema = Schema.from_db()

# Load specific tables only
schema = Schema.from_db(["users", "orders"])
```

**get(name) → Table | View**

```python
# Get table or view by name
table = schema.get("users")
view = schema.get("user_summary")
```

**create_tables(trn=None)**

```python
# Create all tables in current transaction
with Transaction(db):
    schema.create_tables()
```

**reset()**

```python
# Clear all tables and views
schema.reset()
```

### Transaction Class

#### Constructor

```python
Transaction(
    dsn: str = None,
    fk_cache: bool = False
)
```

#### Usage

```python
# Basic usage
with Transaction("sqlite://data.db"):
    # Database operations

# With foreign key caching
with Transaction("sqlite://data.db", fk_cache=True):
    # Optimized for bulk upserts with foreign keys

# Use environment variable
with Transaction():  # Uses NAGRA_DB environment variable
    pass
```

#### Class Methods

**current() → Transaction**

```python
# Get current transaction
trn = Transaction.current()
```

### Select Class

#### Methods

**where(\*expressions) → Select**

```python
select.where("(> age 18)")
select.where("(= status 'active')", "(> balance 0)")
```

**groupby(\*columns) → Select**

```python
select.groupby("department")
select.groupby("city", "year")
```

**orderby(\*columns) → Select**

```python
select.orderby("name")
select.orderby("name asc", "age desc")
```

**limit(n) → Select**

```python
select.limit(10)
```

**execute(\*params) → ResultSet**

```python
# Execute without parameters
results = select.execute()

# Execute with parameters
results = select.execute("Brussels", 2023)
```

**to_pandas() → DataFrame**

```python
df = select.to_pandas()
```

**to_dict() → List[dict]**

```python
records = select.to_dict()
```

**to_dataclass(\*aliases) → type**

```python
DataClass = select.to_dataclass()
DataClass = select.to_dataclass("user_name", "user_email")
```

### Upsert Class

#### Methods

**execute(\*values)**

```python
upsert.execute("john@example.com", "John Doe")
```

**executemany(values)**

```python
upsert.executemany([
    ("john@example.com", "John Doe"),
    ("jane@example.com", "Jane Smith")
])
```

**from_pandas(df)**

```python
upsert.from_pandas(dataframe)
```

**from_dict(records)**

```python
upsert.from_dict([
    {"email": "john@example.com", "name": "John Doe"},
    {"email": "jane@example.com", "name": "Jane Smith"}
])
```

### View Class

#### Constructor

```python
View(
    name: str,
    view_columns: Optional[dict] = None,
    columns: Optional[dict] = None,
    natural_key: Optional[list[str]] = None,
    foreign_keys: Optional[dict] = None,
    as_select: Optional[str] = None,
    view_select: Optional[str] = None,
    schema: Schema = Schema.default,
)
```

#### Class Methods

**get(name, schema=Schema.default) → View**

```python
view = View.get("user_summary")
```

## Migration Guide

### From Version 0.4 to 0.5

1. **Views Support**: New view functionality added

   ```toml
   # New in 0.5: Views
   [user_summary]
   view_select = "users"
   [user_summary.view_columns]
   full_name = "(concat first_name ' ' last_name)"
   email = "email"
   ```

2. **New Methods**:

   - `Upsert.from_dict()`
   - `Upsert.resolve()`
   - String operators: `length`, `upper`, `lower`

3. **Breaking Changes**:
   - `Transaction.current` is now a function: `Transaction.current()`

### Database Migration

When changing schema definitions, you may need to migrate existing databases:

```python
# 1. Create new schema version
new_schema = Schema.from_toml("schema_v2.toml")

# 2. Create migration script
with Transaction("postgresql://user:pass@host/db"):
    # Add new columns
    new_schema.create_tables()  # Only creates missing tables/columns

    # Migrate data if needed
    # ... custom migration logic ...
```

### Schema Evolution Best Practices

1. **Additive Changes**: Add new columns with defaults

   ```toml
   [users]
   [users.columns]
   email = "varchar"
   name = "varchar"
   created_at = "timestamp"  # New column
   [users.default]
   created_at = "now()"
   ```

2. **Backwards Compatibility**: Keep old column names for transition periods

3. **Migration Scripts**: Write scripts for complex schema changes

## Troubleshooting

### Common Issues

#### Schema Definition Errors

**Error**: `IncorrectSchema: Table 'users': Natural key column 'email' not found in columns`

```python
# Problem: Natural key references non-existent column
Table("users", columns={"name": "varchar"}, natural_key=["email"])

# Solution: Ensure natural key columns exist
Table("users", columns={"name": "varchar", "email": "varchar"}, natural_key=["email"])
```

**Error**: `IncorrectSchema: Foreign key 'department' refers to table natural key`

```python
# Problem: Self-referencing foreign key with same name as natural key
Table("users",
      columns={"email": "varchar"},
      natural_key=["email"],
      foreign_keys={"email": "users"})  # Error!

# Solution: Use different column name for foreign key
Table("users",
      columns={"email": "varchar", "manager": "varchar"},
      natural_key=["email"],
      foreign_keys={"manager": "users"})
```

#### Database Connection Issues

**Error**: Connection timeouts or failures

```python
# Check database URL format
# SQLite: sqlite://path/to/file.db
# PostgreSQL: postgresql://user:password@host:port/database

# Verify database exists and permissions are correct
# For PostgreSQL, ensure database and user exist
```

#### Query Errors

**Error**: Column not found in SELECT

```python
# Problem: Referencing non-existent column
users.select("non_existent_column")

# Solution: Check table definition and column names
print(list(users.columns.keys()))
```

**Error**: Foreign key resolution fails

```python
# Problem: Referenced table doesn't exist
orders.select("customer.name")  # Error if 'customers' table not defined

# Solution: Ensure referenced table exists in schema
Table("customers", columns={"name": "varchar"}, natural_key=["name"])
```

#### Performance Issues

**Problem**: Slow upserts with foreign keys

```python
# Solution: Enable foreign key caching
with Transaction(db, fk_cache=True):
    table.upsert("foreign_key.name", "value").executemany(large_dataset)
```

**Problem**: Large result sets consuming memory

```python
# Problem: Loading all results at once
results = list(table.select())

# Solution: Use generators or limit results
for row in table.select().limit(1000):
    process(row)
```

### Debug Mode

Enable debug logging to troubleshoot issues:

```bash
export NAGRA_DEBUG=1
```

```python
import os
os.environ["NAGRA_DEBUG"] = "1"

# Now Nagra will log SQL statements and other debug information
```

### Getting Help

1. **Check the Examples**: Review the `examples/` directory for working code
2. **Read the Tests**: The `tests/` directory contains comprehensive examples
3. **Enable Debug Logging**: Use `NAGRA_DEBUG=1` to see generated SQL
4. **Check Database Logs**: Review database server logs for SQL errors

### Common SQL Patterns

Understanding the SQL that Nagra generates can help debug issues:

```python
# Simple select
users.select("name").stm()
# → SELECT "users"."name" FROM "users"

# With foreign key
orders.select("customer.name").stm()
# → SELECT "customers_0"."name" FROM "orders"
#   LEFT JOIN "customers" as customers_0 ON (customers_0.id = "orders"."customer")

# With aggregation
sales.select("region", "(sum amount)").groupby("region").stm()
# → SELECT "sales"."region", sum("sales"."amount") FROM "sales" GROUP BY "sales"."region"
```

Use the `.stm()` method on any query object to see the generated SQL.

---

This documentation covers the complete functionality of the Nagra ORM library. For the most up-to-date information and examples, refer to the project repository and changelog.
