import re
from collections.abc import Iterable
import dataclasses
from datetime import datetime, date
from itertools import islice, repeat, takewhile
from typing import Optional, Union, TYPE_CHECKING

from nagra import Statement
from nagra.exceptions import ValidationError
from nagra.sexpr import AST, AggToken

if TYPE_CHECKING:
    from nagra.table import Env
    from nagra.transaction import Transaction
    from pandas import DataFrame

RE_VALID_IDENTIFIER = re.compile(r"\W|^(?=\d)")


def clean_col(name):
    return RE_VALID_IDENTIFIER.sub("_", name)


class Select:
    def __init__(self, table, *columns: str, trn: "Transaction", env: "Env"):
        self.table = table
        self.env = env
        self.where_asts = tuple()
        self._offset = None
        self._limit = None
        self._aliases = tuple()
        self.groupby_ast = tuple()
        self.order_ast = tuple()
        self.order_directions = tuple()
        self.columns = tuple()
        self.columns_ast = tuple()
        self.query_columns = tuple()
        self.trn = trn
        self._add_columns(columns)

    def _add_columns(self, columns):
        self.columns += columns
        self.columns_ast += tuple(AST.parse(c) for c in columns)
        self.query_columns += tuple(
            a.eval(self.env, self.trn.flavor) for a in self.columns_ast
        )

    def clone(self, trn: Optional["Transaction"] = None):
        """
        Return a copy of select with updated parameters
        """
        trn = trn or self.trn
        cln = Select(self.table, *self.columns, trn=trn, env=self.env.clone())
        cln.where_asts = self.where_asts
        cln.groupby_ast = self.groupby_ast
        cln.order_ast = self.order_ast
        cln.order_directions = self.order_directions
        cln._limit = self._limit
        cln._offset = self._offset
        cln._aliases = self._aliases
        return cln

    def where(self, *conditions: str):
        cln = self.clone()
        cln.where_asts += tuple(AST.parse(cond) for cond in conditions)
        return cln

    def aliases(self, *names: str):
        cln = self.clone()
        cln._aliases += names
        return cln

    def select(self, *columns: str):
        cln = self.clone()
        cln._add_columns(columns)
        return cln

    def offset(self, value):
        cln = self.clone()
        cln._offset = value
        return cln

    def limit(self, value):
        cln = self.clone()
        cln._limit = value
        return cln

    def groupby(self, *groups: str):
        cln = self.clone()
        cln.groupby_ast += tuple(AST.parse(g) for g in groups)
        return cln

    def orderby(self, *orders: str | tuple[str, str]):
        expressions = []
        directions = []
        for o in orders:
            if isinstance(o, tuple):
                expression = o[0]
                direction = o[1]
            else:
                expression = o
                direction = "asc"

            if isinstance(expression, int):
                expression = self.columns[expression]
            expressions.append(expression)
            directions.append(direction)

        cln = self.clone()
        cln.order_ast += tuple(AST.parse(e) for e in expressions)
        cln.order_directions += tuple(directions)
        return cln

    def to_dataclass(self, *aliases: str, model_name=None, nest=False):
        fields = {}
        for col, (name, dt) in zip(self.columns, self.dtypes(*aliases)):
            # Handle nested models
            if nest and "." in col:
                head, _ = col.split(".", 1)
                if head in fields:
                    # Fields for this fk already managed
                    continue
                # Create nested fields
                fname = self.table.foreign_keys[head]
                ftable = self.table.schema.get(fname)
                tails = []
                for sub_col in self.columns:
                    prefix = f"{head}."
                    if sub_col.startswith(prefix):
                        tails.append(sub_col.removeprefix(prefix))
                # trigger a select on foreign table and generate dataclass
                sub_class = ftable.select(*tails).to_dataclass(nest=True)
                nullable = not self.table.required(head)
                if nullable:
                    sub_class = sub_class | None
                field_def = (
                    head,
                    sub_class,
                )
                if nullable:
                    field_def += (None,)
                fields[head] = field_def

            # Handle required/optional fields
            else:
                field_def = (
                    clean_col(name),
                    dt,
                )
                if not self.table.required(name):
                    field_def += (None,)
                fields[name] = field_def

        return dataclasses.make_dataclass(
            model_name or self.table.name, fields=fields.values()
        )

    def to_pydantic(self, *aliases: str, model_name=None):
        from pydantic import create_model

        fields = {}
        for name, dt in self.dtypes(*aliases):
            cleaned = clean_col(name)
            if self.table.required(name):
                field_def = (dt, ...)
            else:
                # Add default value
                field_def = (dt, None)
            fields[cleaned] = field_def

        return create_model(model_name or self.table.name, **fields)

    def dtypes(self, *aliases: str, with_optional: bool = True):
        fields = []
        if aliases:
            assert len(aliases) == len(self.columns)
        else:
            aliases = self.columns

        # Construct fields
        for alias, col_name, col_ast in zip(aliases, self.columns, self.columns_ast):
            # Eval type and nullable
            col_type = col_ast.eval_type(self.env)
            nullable = col_ast.is_nullable(self.env)
            # Eval nullable
            if with_optional and nullable:
                # Fixme Optional may depend on ast content
                col_type = col_type | None
            fields.append((alias, col_type))
        return fields

    def infer_groupby(self):
        # Detect aggregates
        for a in self.columns_ast:
            if any(isinstance(tk, AggToken) for tk in a.chain()):
                break
        else:
            # No aggregate found
            return []

        # Collect non-aggregates
        groupby_ast = []
        for a in self.columns_ast:
            if any(isinstance(tk, AggToken) for tk in a.chain()):
                continue
            groupby_ast.append(a)
        return groupby_ast

    def stm(self):
        # Eval where conditions
        where_conditions = [
            ast.eval(self.env, self.trn.flavor) for ast in self.where_asts
        ]
        # Eval Groupby
        groupby_ast = self.groupby_ast or self.infer_groupby()
        groupby = [a.eval(self.env, self.trn.flavor) for a in groupby_ast]
        # Eval Oder by
        orderby = [
            a.eval(self.env, self.trn.flavor) + f" {d}"
            for a, d in zip(
                self.order_ast,
                self.order_directions,
            )
        ]
        # Create joins
        joins = self.table.join(self.env)

        if self._aliases:
            query_columns = [
                f"{c} AS {a}" for c, a in zip(self.query_columns, self._aliases)
            ]
        else:
            query_columns = self.query_columns

        stm = Statement(
            "select",
            table=self.table.name,
            columns=query_columns,
            joins=joins,
            conditions=where_conditions,
            limit=self._limit,
            offset=self._offset,
            groupby=groupby,
            orderby=orderby,
        )
        return stm()

    def to_pandas(
        self, *args, chunked: int = 0
    ) -> Union["DataFrame", Iterable["DataFrame"]]:
        """
        Execute the query with given args and return a pandas
        DataFrame. If chunked is bigger than 0, return an iterable
        yielding dataframes.
        """
        names, dtypes = zip(*(self.dtypes(with_optional=False)))
        cursor = self.execute(*args)
        if chunked <= 0:
            return self.create_df(cursor, names, dtypes)

        # Create generator
        chunkify = (list(islice(cursor, chunked)) for _ in repeat(None))
        # Return df as long as the generator yield non-empty list
        return (
            self.create_df(chunk, names, dtypes) for chunk in takewhile(bool, chunkify)
        )

    @classmethod
    def create_df(cls, cursor: Iterable[tuple], names: tuple[str, ...], dtypes: tuple):
        """
        Create a Dataframe, whose columns name are `names` and
        types `dtypes`.
        """
        from pandas import DataFrame, Series, to_datetime

        df = DataFrame()
        by_col = zip(*cursor)
        for name, dt, col in zip(names, dtypes, by_col):
            # FIXME Series(col, dtype=dt) fail on json cols!
            srs = Series(col)
            if dt in (datetime, date):
                srs = to_datetime(srs)
            else:
                if dt == int:
                    # Make sure we have no nan for int columns
                    srs = srs.fillna(0)
                try:
                    srs = srs.astype(dt)
                except TypeError:
                    # Fallback to string if type is not supported by pandas
                    srs = srs.astype(str)
            df[name] = srs

        if df.columns.empty:
            # No records were returned by the cursor
            return DataFrame(columns=names)

        return df

    def to_dict(self, *args, nest=False) -> Iterable[dict]:
        if nest:
            if self._aliases:
                msg = "Nesting and fields aliases can not be combined"
                raise ValidationError(msg)
            yield from self.to_nested_dict(*args)
        else:
            columns = [f.name for f in dataclasses.fields(
                self.to_dataclass(*self._aliases)
            )]
            for row in self.execute(*args):
                yield dict(zip(columns, row))

    def to_nested_dict(self, *args) -> Iterable[dict]:
        for row in self.execute(*args):
            record = dict(zip(self.columns, row))
            yield autonest(record)


    def execute(self, *args):
        return self.trn.execute(self.stm(), args)

    def executemany(self, args):
        return self.trn.executemany(self.stm(), args)

    def __iter__(self):
        return iter(self.execute())


def autonest(record: dict) -> dict:
    clone = {}
    for key, value in record.items():
        if "." not in key:
            clone[key] = value
            continue
        head, _ = key.split(".", 1)
        prefix = f"{head}."
        sub_dict = {
            k.removeprefix(prefix): v
            for k, v in record.items()
            if k.startswith(prefix)
        }
        if all(v is None for v in sub_dict.values()):
            # We only collect sub_dict if not fully null
            clone[head] = None
            continue
        clone[head] = autonest(sub_dict)
    return clone
