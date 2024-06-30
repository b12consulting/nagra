from collections import defaultdict
from collections.abc import Iterable
from itertools import islice

from nagra.exceptions import UnresolvedFK, ValidationError
from nagra.utils import logger
from nagra.transaction import ExecMany

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None


class WriterMixin:
    """
    Utility class that provide common methods for Update and Upsert
    """

    def __init__(self):
        self.groups, self.resolve_stm = self.prepare()

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
            select = ftable.select(ftable.primary_key).where(*cond)
            resolve_stm[col] = select.stm()
        return groups, resolve_stm

    def execute(self, *values):
        ids = self.executemany([values])
        if ids:
            return ids[0]

    def executemany(self, records: Iterable[tuple]):
        # Transform list of records into a dataframe-like dict
        value_df = dict(zip(self.columns, zip(*records)))
        arg_df = {}
        for col, to_select in self.groups.items():
            if to_select:
                values = list(zip(*(value_df[f"{col}.{s}"] for s in to_select)))
                arg_df[col] = self._resolve(col, values)
            else:
                arg_df[col] = value_df[col]

        # Build arg iterable
        args = self._exec_args(arg_df)
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

    def validate(self, ids: list[int]):
        iter_ids = iter(ids)
        pk = self.table.primary_key
        while True:
            chunk = list(islice(iter_ids, 1000))
            if not chunk:
                return
            cond = self._where + [f"(in {pk} %s)" % (" {}" * len(chunk))]
            select = self.table.select("(count)").where(*cond)
            (count,) = select.execute(*chunk).fetchone()
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

    def from_pandas(self, df: "DataFrame"):
        # Convert non-basic types to string
        is_copy = False
        for col in self.columns:
            if df[col].dtype in ("int", "float", "bool", "str"):
                continue
            if not is_copy:
                df = df.copy()
                is_copy = True
            df[col] = df[col].astype(str)

        rows = df[self.columns].values
        return self.executemany(rows)
