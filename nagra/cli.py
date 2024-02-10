import argparse
import os

from tabulate import tabulate

from nagra import Transaction, Table, Schema
from nagra import load_schema


def select(args):
    select = Table.get(args.table).select(*args.columns)
    if args.where:
        select.where(*args.where)
    if args.limit:
        select = select.limit(args.limit)
    if args.orderby:
        select = select.orderby(args.orderby)
    rows = list(select.execute())
    headers = [d[0] for d in select.dtypes()]
    print(tabulate(rows, headers))


def delete(args):
    delete = Table.get(args.table).delete()
    delete.where(*args.where)
    delete.execute()


def schema(args):
    sch = Schema.default
    if args.d2:
        print(sch.generate_d2())
        return

    rows = []
    for name in sch.tables:
        rows.append([name])
    headers = ["table"]
    print(tabulate(rows, headers))


def run():
    # top-level parser
    parser = argparse.ArgumentParser(
        prog="nagra",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    default_db = os.environ.get('NAGRA_DB')
    default_schema = os.environ.get('NAGRA_SCHEMA')
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
    subparsers = parser.add_subparsers(dest="command")

    parser_select = subparsers.add_parser("select")
    parser_select.add_argument("table")
    parser_select.add_argument("columns", nargs="*")
    parser_select.add_argument("--where", "-W", type=str, nargs="*")
    parser_select.add_argument("--limit", "-L", type=int)
    parser_select.add_argument("--orderby", "-O", help="Order by given column")
    parser_select.set_defaults(func=select)

    parser_delete = subparsers.add_parser("delete")
    parser_delete.add_argument("table")
    parser_delete.add_argument("--where", "-W", type=str, nargs="*")
    parser_delete.set_defaults(func=delete)

    parser_schema = subparsers.add_parser("schema")
    parser_schema.add_argument("--d2", action="store_true",
                               help="Generate d2 file")
    parser_schema.set_defaults(func=schema)

    # Parse args
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    try:
        with Transaction(args.db):
            load_schema(args.schema)
            args.func(args)
    except (BrokenPipeError, KeyboardInterrupt):
        pass
