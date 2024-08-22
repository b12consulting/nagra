import argparse
import csv
import os
import sys
from itertools import chain

from nagra import Transaction, Schema, __version__
from nagra.utils import print_table


def select(args, schema):
    select = schema.get(args.table).select(*args.columns)
    if args.where:
        where = chain.from_iterable(args.where)
        select = select.where(*where)
    if args.limit:
        select = select.limit(args.limit)
    if args.orderby:
        orderby = chain.from_iterable(args.orderby)
        select = select.orderby(*orderby)
    rows = list(select.execute())
    headers = [d[0] for d in select.dtypes()]
    if args.csv:
        writer = csv.writer(sys.stdout)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)
    else:
        print_table(rows, headers, args.pivot)


def delete(args, schema):
    delete = schema.get(args.table).delete()
    if args.where:
        where = chain.from_iterable(args.where)
        delete = delete.where(*where)
    delete.execute()


def print_schema(args, schema):
    if args.d2:
        print(schema.generate_d2())
        return

    # If tables name are given, print details
    if args.tables:
        rows = []
        headers = ["table", "column", "type"]
        for table_name in args.tables:
            for col, dtype in schema.get(table_name).columns.items():
                rows.append([table_name, col, dtype])
        print_table(rows, headers, args.pivot)
        return

    # List all tables
    rows = []
    for name in sorted(schema.tables.keys()):
        rows.append([name])
    headers = ["table"]
    print_table(rows, headers, args.pivot)


def show_version():
    print(__version__)


def run():
    # top-level parser
    parser = argparse.ArgumentParser(
        prog="nagra",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    default_db = os.environ.get("NAGRA_DB")
    default_schema = os.environ.get("NAGRA_SCHEMA")
    parser.add_argument(
        "--db",
        "-d",
        default=default_db,
        help=f"DB uri, (default: {default_db})",
    )
    parser.add_argument(
        "--schema",
        "-s",
        default=default_schema,
        help=f"DB schema, (default: {default_schema})",
    )
    parser.add_argument(
        "--pivot",
        "-p",
        action="store_true",
        help="Pivot results (one key-value table per record)",
    )
    parser.add_argument(
        "--version",
        "-V",
        action="store_true",
        help="Show version",
    )
    subparsers = parser.add_subparsers(dest="command")

    parser_select = subparsers.add_parser("select")
    parser_select.add_argument("table")
    parser_select.add_argument("columns", nargs="*")
    parser_select.add_argument("--where", "-W", type=str, action="append", nargs="*")
    parser_select.add_argument("--limit", "-L", type=int)
    parser_select.add_argument(
        "--orderby",
        "-O",
        type=str,
        action="append",
        nargs="*",
        help="Order by given columns",
    )
    parser_select.add_argument(
        "--csv",
        action="store_true",
        help="Format output as csv",
    )
    parser_select.set_defaults(func=select)

    parser_delete = subparsers.add_parser("delete")
    parser_delete.add_argument("table")
    parser_delete.add_argument("--where", "-W", type=str, action="append", nargs="*")
    parser_delete.set_defaults(func=delete)

    parser_schema = subparsers.add_parser("schema")
    parser_schema.add_argument("--d2", action="store_true", help="Generate d2 file")
    parser_schema.add_argument("tables", nargs="*")
    parser_schema.set_defaults(func=print_schema)

    # Parse args
    args = parser.parse_args()
    if args.version:
        show_version()
        return
    if not args.command:
        parser.print_help()
        return

    try:
        with Transaction(args.db):
            if args.schema:
                schema = Schema.from_toml(open(args.schema))
            else:
                schema = Schema.from_db()
            args.func(args, schema=schema)
    except (BrokenPipeError, KeyboardInterrupt):
        pass
