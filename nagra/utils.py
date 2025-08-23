import csv
import os
import bisect
import logging
import re
import sys
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter
from enum import StrEnum

from jinja2 import FileSystemLoader, Environment
from rich import box
from rich.console import Console
from rich.markup import escape
import rich.table


HERE = Path(__file__).parent
fmt = "%(levelname)s:%(asctime).19s: %(message)s"
logging.basicConfig(format=fmt)
logger = logging.getLogger("nagra")
if os.environ.get("NAGRA_DEBUG"):
    logger.setLevel("DEBUG")
    logger.debug("Log level set to debug")
UNSET = object()
RE_SC_PC = re.compile(r"(?:^|_)(\w)")


def autoquote(x):
    if x.startswith('"'):
        return x
    return f'"{x}"'


# Setup jinja env
jinja_env = Environment(loader=FileSystemLoader(HERE / "template"))
jinja_env.filters["autoquote"] = autoquote


def snake_to_pascal(name):
    return RE_SC_PC.sub(lambda m: m.group(1).upper(), name)


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
