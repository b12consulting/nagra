import argparse
import os
import sys
from itertools import chain

from nagra import Transaction, Schema, View, __version__
from nagra.utils import print_table


def select(args, schema):
    table = schema.get(args.table)
    if args.columns:
        cols = args.columns
    else:
        # Ignore blob col by default
        cols = [n for n, c in table.columns.items() if c.dtype != "blob"]

    select = table.select(*cols)
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

    print_table(rows, headers, args.pivot, format=args.table_fmt)


def delete(args, schema):
    delete = schema.get(args.table).delete()
    if args.where:
        where = chain.from_iterable(args.where)
        delete = delete.where(*where)
    delete.execute()


def print_schema(args, schema):
    if args.fmt == "d2":
        print(schema.generate_d2())
        return

    elif args.fmt == "toml":
        print(schema.generate_toml())
        return

    elif args.fmt == "pydantic":
        print(schema.generate_pydantic_models(table_names=args.tables))
        return

    # If tables name are given, print details
    if args.tables:
        rows = []
        headers = ["table", "column", "type"]
        for table_name in args.tables:
            for col in schema.get(table_name).columns.values():
                rows.append([table_name, col.name, col.dtype])
        print_table(rows, headers, args.pivot, format=args.table_fmt)
        return

    # List all tables
    rows = [(tbl.name, tbl.is_view) for tbl in schema.tables.values()]
    headers = ["table", "view"]
    print_table(sorted(rows), headers, args.pivot, format=args.table_fmt)


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
    parser.add_argument(
        "--csv",
        action="store_const",
        const="csv",
        dest="table_fmt",
        help="Format output as csv",
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
    parser_select.set_defaults(func=select)

    parser_delete = subparsers.add_parser("delete")
    parser_delete.add_argument("table")
    parser_delete.add_argument("--where", "-W", type=str, action="append", nargs="*")
    parser_delete.set_defaults(func=delete)

    parser_schema = subparsers.add_parser("schema")
    parser_schema.add_argument(
        "--d2",
        help="Generate d2 file",
        dest="fmt",
        action="store_const",
        const="d2",
        default=None,
    )
    parser_schema.add_argument(
        "--toml",
        help="Generate toml file",
        dest="fmt",
        action="store_const",
        const="toml",
        default=None,
    )
    parser_schema.add_argument("tables", nargs="*")
    parser_schema.add_argument(
        "--pydantic",
        dest="fmt",
        action="store_const",
        const="pydantic",
        default=None,
        help="Generate pydantic models for the schema",
    )
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
