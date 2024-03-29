from collections import defaultdict
from itertools import islice

from nagra import Statement
from nagra.sexpr import AST
from nagra.exceptions import UnresolvedFK, ValidationError
from nagra.utils import logger
from nagra.transaction import ExecMany

class Upsert:
    def __init__(self, table, *columns, trn, lenient=None):
        self.table = table
        self.columns = list(columns)
        self.columns_ast = [AST.parse(c) for c in columns]
        self.groups, self.resolve_stm = self.prepare()
        self._insert_only = False
        self.lenient = lenient or []
        self._where = None
        self.trn = trn

    def stm(self):
        conflict_key = ["id"] if "id" in self.groups else self.table.natural_key
        columns = self.groups
        do_update = False if self._insert_only else len(columns) > len(conflict_key)
        stm = Statement(
            "upsert",
            self.trn.flavor,
            table=self.table.name,
            columns=columns,
            conflict_key=conflict_key,
            do_update=do_update,
        )
        return stm()

    def insert_only(self):
        # TODO clone first
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
            ftable = self.table.schema.get(self.table.foreign_keys[col])
            select = ftable.select("id").where(*cond)
            resolve_stm[col] = select.stm()
        return groups, resolve_stm

    def where(self, *conditions):
        if self._where is None:
            self._where = []
        self._where.extend(conditions)
        return self

    def execute(self, *values):
        ids = self.executemany([values])
        if ids:
            return ids[0]

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
        ids = []
        while True:
            chunk = list(islice(args, 1000))
            if not chunk:
                break
            if self.trn.flavor == "sqlite":
                for item in chunk:
                    cursor = self.trn.execute(stm, item)
                    new_id = cursor.fetchone()
                    ids.append(new_id[0] if new_id else None)
            else:
                cursor = self.trn.executemany(stm, chunk)
                while True:
                    new_id = cursor.fetchone()
                    ids.append(new_id[0] if new_id else None)
                    if not cursor.nextset():
                        break

        # If conditions are present, enforce those
        if self._where:
            self.validate(ids)
        return ids

    def validate(self, ids):
        iter_ids = iter(ids)
        while True:
            chunk = list(islice(iter_ids, 1000))
            if not chunk:
                return
            cond = self._where + ["(in id %s)" % (" {}" * len(chunk))]
            select = self.table.select("(count)").where(*cond)
            count, = select.execute(*chunk).fetchone()
            if count != len(chunk):
                msg = f"Validation failed! Condition is: {self._where} )"
                raise ValidationError(msg)

    def _resolve(self, col, values):
        # XXX Detect situation where more than on result is found for
        # a given value (we could also enforce that we only resolve
        # columns with unique constraints) ?
        stm = self.resolve_stm[col]
        exm = ExecMany(stm, values, trn=self.trn)
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
                raise UnresolvedFK(
                    f"Unable to resolve '{vals}' (for foreign key "
                    f"{col} of table {self.table.name})"
                )

    def __call__(self, records):
        return self.executemany(records)

    def from_pandas(self, df:"DataFrame"):
        rows =  df[self.columns].values
        self.executemany(rows)
