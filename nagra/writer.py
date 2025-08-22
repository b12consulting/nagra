import dataclasses
from collections import defaultdict
from collections.abc import Iterable
from functools import partial
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
        if not value_df:
            return []
        arg_df = {}
        for col, to_select in self.groups.items():
            if to_select:
                values = list(zip(*(value_df[f"{col}.{s}"] for s in to_select)))
                # Try to instanciate lru cache
                cache_key = (self.resolve_stm[col], str(self.lenient))
                lru = self.trn.get_fk_cache(cache_key, fn=partial(self._resolve, col))
                if lru is not None:
                    arg_df[col] = lru.run(values)
                else:
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
                returning = self.table.primary_key is not None
                cursor = self.trn.executemany(stm, chunk, returning)
                if returning:
                    while True:
                        new_id = cursor.fetchone()
                        ids.append(new_id[0] if new_id else None)
                        if not cursor.nextset():
                            break

        # If conditions are present, enforce those
        if self._check:
            self.validate(ids)
        return ids

    def validate(self, ids: list[int]):
        iter_ids = iter(ids)
        pk = self.table.primary_key
        while True:
            chunk = list(islice(iter_ids, 1000))
            if not chunk:
                return
            cond = self._check + [f"(in {pk} %s)" % (" {}" * len(chunk))]
            select = self.table.select("(count *)").where(*cond)
            (count,) = select.execute(*chunk).fetchone()
            if count != len(chunk):
                msg = f"Validation failed! Condition is: {self._check} )"
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

    def from_dict(self, records):
        # Create select object in order to generate the same column names
        select = self.table.select(*self.columns)
        field_names = [f.name for f in dataclasses.fields(select.to_dataclass())]

        # Extract dict values - allows for field names or dotted column format
        f_or_c = zip(field_names, self.columns)
        rows = (tuple(getter(record, field, col) for col, field in f_or_c) for record in records)
        return self.executemany(rows)


def getter(record, field, col):
    """
    Utility function to extract value as a column name (with
    potentially dots) or a field name from record
    """
    if field in record:
        return record[field]
    if col in record:
        return record[col]
    raise KeyError(f"KeyError: neither {field} or {col} found")
