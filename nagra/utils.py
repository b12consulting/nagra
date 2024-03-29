import os
import bisect
import logging
import sys
from contextlib import contextmanager
from pathlib import Path
from time import perf_counter


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


def autoquote(x):
    if x.startswith('"'):
        return x
    return f'"{x}"'


# Setup jinja env
jinja_env = Environment(loader=FileSystemLoader(HERE / "template"))
jinja_env.filters["autoquote"] = autoquote


def template(name, ext="sql"):
    return jinja_env.get_template(f"{name}.{ext}")


def strip_lines(stmt):
    for r in stmt.splitlines():
        line = r.strip()
        if line:
            yield line


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


def print_table(rows, headers, pivot=False):
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
