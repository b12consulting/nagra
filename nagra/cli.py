from itertools import chain
import argparse
import os

from nagra import Transaction, Schema, Table
from nagra.utils import print_table


def select(args):
    select = Table.get(args.table).select(*args.columns)
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
    print_table(rows, headers, args.pivot)


def delete(args):
    delete = Table.get(args.table).delete()
    where = chain.from_iterable(args.where)
    delete.where(*where)
    delete.execute()


def schema(args):
    sch = Schema.default
    if args.d2:
        print(sch.generate_d2())
        return

    # If tables name are given, print details
    if args.tables:
        rows = []
        headers = ["table", "column", "type"]
        for table_name in args.tables:
            for col, dtype in sch.get(table_name).columns.items():
                rows.append([table_name, col, dtype])
        print_table(rows, headers, args.pivot)
        return

    # List all tables
    rows = []
    for name in sorted(sch.tables.keys()):
        rows.append([name])
    headers = ["table"]
    print_table(rows, headers, args.pivot)


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
    parser.add_argument(
        "--pivot",
        "-p",
        action="store_true",
        help='Pivot results (one key-value table per record)',
    )
    subparsers = parser.add_subparsers(dest="command")

    parser_select = subparsers.add_parser("select")
    parser_select.add_argument("table")
    parser_select.add_argument("columns", nargs="*")
    parser_select.add_argument("--where", "-W", type=str, action='append', nargs="*")
    parser_select.add_argument("--limit", "-L", type=int)
    parser_select.add_argument("--orderby", "-O", type=str,
                               action='append', nargs="*", help="Order by given columns")
    parser_select.set_defaults(func=select)

    parser_delete = subparsers.add_parser("delete")
    parser_delete.add_argument("table")
    parser_delete.add_argument("--where", "-W", type=str, action='append', nargs="*")
    parser_delete.set_defaults(func=delete)

    parser_schema = subparsers.add_parser("schema")
    parser_schema.add_argument("--d2", action="store_true",
                               help="Generate d2 file")
    parser_schema.add_argument("tables", nargs="*")
    parser_schema.set_defaults(func=schema)

    # Parse args
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    try:
        with Transaction(args.db):
            Schema.default.load(open(args.schema))
            args.func(args)
    except (BrokenPipeError, KeyboardInterrupt):
        pass
