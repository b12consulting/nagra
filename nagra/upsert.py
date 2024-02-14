from collections import defaultdict
from itertools import islice

from nagra import Statement, executemany, execute, Transaction
from nagra.transaction import ExecMany
from nagra.sexpr import AST
from nagra.schema import Schema
from nagra.exceptions import UnresolvedFK
from nagra.utils import logger


class Upsert:
    def __init__(self, table, *columns, lenient=None):
        self.table = table
        self.columns = columns
        self.columns_ast = [AST.parse(c) for c in columns]
        self.groups, self.resolve_stm = self.prepare()
        self._insert_only = False
        self.lenient = lenient or []

    def stm(self):
        conflict_key = ["id"] if "id" in self.groups else self.table.natural_key
        columns = self.groups
        do_update = False if self._insert_only else len(columns) > len(conflict_key)
        stm = Statement(
            "upsert",
            table=self.table.name,
            columns=columns,
            conflict_key=conflict_key,
            do_update=do_update,
        )

        # TODO add returning id to upsert sql and re-query those ids to make sure acl are respected

        return stm()

    def insert_only(self):
        self._insert_only = True
        return self

    def prepare(self):
        """
        Organise columns in groups and prepare statement to
        resolve fk based on columns expressions
        """
        groups = defaultdict(list)
        for col in self.columns:
            if "." in col:
                head, tail = col.split(".", 1)
                groups[head].append(tail)
            else:
                groups[col] = None

        resolve_stm = {}
        for col, to_select in groups.items():
            if not to_select:
                continue
            cond = ["(= %s {})" % c for c in to_select]
            ftable = Schema.get(self.table.foreign_keys[col])
            select = ftable.select("id").where(*cond)
            resolve_stm[col] = select.stm()
        return groups, resolve_stm

    def execute(self, *values):
        self.executemany([values])

    def executemany(self, records):
        # Transform list of records into a dataframe-like dict
        value_df = dict(zip(self.columns, zip(*records)))
        arg_df = {}
        for col, to_select in self.groups.items():
            if to_select:
                values = list(zip(*(value_df[f"{col}.{s}"] for s in to_select)))
                arg_df[col] = self._resolve(col, values)
            else:
                arg_df[col] = value_df[col]

        args = zip(*(arg_df[c] for c in self.groups))
        # Work by chunks
        stm = self.stm()
        while True:
            chunk = list(islice(args, 1000))
            if not chunk:
                break
            if Transaction.flavor == "sqlite":
                for item in chunk:
                    execute(stm, item)
            else:
                executemany(stm, chunk)

    def _resolve(self, col, values):
        stm = self.resolve_stm[col]
        exm = ExecMany(stm, values)
        for res, vals in zip(exm, values):
            if res is not None:
                yield res[0]
            elif any(v is None for v in vals):
                # One of the values is not given
                yield None
            elif self.lenient is True or col in self.lenient:
                msg = "Value '%s' not found for foreign key column '%s' of table %s"
                logger.info(msg, vals, col, self.table)
                yield None
            else:
                raise UnresolvedFK(f"Unable to resolve '{vals}' (for column '{col}')")

    def __call__(self, records):
        return self.executemany(records)
