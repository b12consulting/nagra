import csv
import os
import bisect
import logging
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
from time import perf_counter
from typing import Iterator, TYPE_CHECKING, get_args
from enum import StrEnum
from urllib.parse import parse_qs, unquote_plus, urlparse

from jinja2 import FileSystemLoader, Environment
from rich import box
from rich.console import Console
from rich.markup import escape
import rich.table

if TYPE_CHECKING:
    from nagra.schema import Schema

HERE = Path(__file__).parent
fmt = "%(levelname)s:%(asctime).19s: %(message)s"
logging.basicConfig(format=fmt)
logger = logging.getLogger("nagra")
if os.environ.get("NAGRA_DEBUG"):
    logger.setLevel("DEBUG")
    logger.debug("Log level set to debug")
UNSET = object()
RE_SC_PC = re.compile(r"(?:^|_)(\w)")
RE_PC_SC = re.compile(r"(?<!^)(?=[A-Z])")


def quote_identifier(name: str, flavor: str | None = None) -> str:
    """
    Quote an identifier according to the given database flavor.
    Defaults to ANSI double quotes when flavor is not specified.
    """
    left, right = ('[', ']') if flavor == "mssql" else ('"', '"')

    if name.startswith(left) and name.endswith(right):
        return name

    if "." in name:
        return ".".join(quote_identifier(part, flavor) for part in name.split("."))

    return f"{left}{name}{right}"


def autoquote(x):
    if x.startswith('"'):
        return x
    return f'"{x}"'


# Setup jinja env
jinja_env = Environment(loader=FileSystemLoader(HERE / "template"))
jinja_env.filters["autoquote"] = autoquote


def snake_to_pascal(name: str) -> str:
    return RE_SC_PC.sub(lambda m: m.group(1).upper(), name)


def pascal_to_snake(name: str) -> str:
    # PascalCase to snake_case
    return RE_PC_SC.sub("_", name).lower()


def template(name):
    return jinja_env.get_template(name)


def strip_lines(stmt):
    res = []
    for r in stmt.splitlines():
        line = r.strip()
        if line:
            res.append(line)
    return res


def pretty_nb(number):
    prefixes = "yzafpnum_kMGTPEZY"
    factors = [1000**i for i in range(-8, 8)]
    if number == 0:
        return 0
    if number < 0:
        return "-" + pretty_nb(-number)
    idx = bisect.bisect_right(factors, number) - 1
    prefix = prefixes[idx]
    return "%.2f%s" % (number / factors[idx], "" if prefix == "_" else prefix)


@contextmanager
def timeit(title=""):
    start = perf_counter()
    yield
    delta = perf_counter() - start
    print(title, pretty_nb(delta) + "s", file=sys.stderr)


class TableFmt(StrEnum):
    RICH = "rich"
    CSV = "csv"


def print_table(rows, headers, pivot=False, format:TableFmt = None):
    if format == TableFmt.CSV:
        writer = csv.writer(sys.stdout)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
        return

    console = Console()
    escstr = lambda s: escape(str(s))
    if pivot:
        for row in rows:
            table = rich.table.Table(box=box.SIMPLE)
            for pivot_row in zip(headers, row):
                table.add_row(*map(escstr, pivot_row))
            console.print(table)
        return

    table = rich.table.Table(*headers, box=box.ROUNDED)
    for row in rows:
        table.add_row(*map(escstr, row))
    console.print(table)


def iter_dataclass_cols(cls: dataclass, prefix: str = "") -> Iterator[str]:
    """
    Return columns corresponding to the dataclass, including
    nested dataclasses (using dotted notation).
    """
    for field in fields(cls):
        name = field.name
        type = field.type
        column = f"{prefix}.{name}" if prefix else name
        sub_types = get_args(type)
        # Handle optional dataclasses
        for sub_type in sub_types:
            if is_dataclass(sub_type):
                yield from iter_dataclass_cols(sub_type, prefix=column)
                break
        else:
            # Handle other types
            if is_dataclass(type):
                yield from iter_dataclass_cols(type, prefix=column)
            else:
                yield column


def get_table_from_dataclass(cls: dataclass, schema: "Schema"):
    assert is_dataclass(cls)
    cls_name = getattr(cls, "__table__", cls.__name__)
    table = schema.get(pascal_to_snake(cls_name))
    return table


def mssql_connection_string(dsn: str) -> str:
    """
    Build an ODBC connection string from a mssql:// style DSN.
    """
    parsed = urlparse(dsn)
    query = parse_qs(parsed.query, keep_blank_values=True)
    q = lambda name, default=None: unquote_plus(
        query.pop(name, [default])[-1]
    )

    driver = q("driver", "ODBC Driver 18 for SQL Server")
    trustservercertificate = q("trust_server_certificate", "no")
    parts = [
        f"DRIVER={{{driver}}}",
        f"TrustServerCertificate={{{trustservercertificate}}}",
    ]

    host = parsed.hostname or "localhost"
    if parsed.port:
        host = f"{host},{parsed.port}"
    parts.append(f"SERVER={host}")

    database = parsed.path.lstrip("/")
    if database:
        parts.append(f"DATABASE={unquote_plus(database)}")

    if parsed.username:
        parts.append(f"UID={unquote_plus(parsed.username)}")
    if parsed.password:
        parts.append(f"PWD={unquote_plus(parsed.password)}")

    return ";".join(parts)
