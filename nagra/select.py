import re
from collections.abc import Iterable
import dataclasses
from datetime import datetime, date
from itertools import islice, repeat, takewhile
from typing import Optional, Union, TYPE_CHECKING

from nagra import Statement
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
        self.env = env  # Env(table)
        self.where_asts = tuple()
        self._offset = None
        self._limit = None
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
        return cln

    def where(self, *conditions: str):
        cln = self.clone()
        cln.where_asts += tuple(AST.parse(cond) for cond in conditions)
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

    def to_dataclass(self, *aliases: str):
        return dataclasses.make_dataclass(
            self.table.name,
            fields=[(clean_col(c), d) for c, d in self.dtypes(*aliases)],
        )

    def dtypes(self, *aliases: str, with_optional: bool = True):
        fields = []
        if aliases:
            assert len(aliases) == len(self.columns)
        else:
            aliases = self.columns

        # Construct fields
        for alias, col_name, col_ast in zip(aliases, self.columns, self.columns_ast):
            # Keep first part of dotted chains
            col_name = col_name.split(".", 1)[0]
            # Eval type
            col_type = col_ast.eval_type(self.env)
            # Eval nullable
            not_natural_key = col_name not in self.table.natural_key
            is_nullable = col_name not in self.table.not_null
            not_pk = col_name != self.table.primary_key
            if with_optional and not_pk and not_natural_key and is_nullable:
                # Fixme Optional may depend on ast content
                col_type = Optional[col_type]
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

        stm = Statement(
            "select",
            table=self.table.name,
            columns=self.query_columns,
            joins=joins,
            conditions=where_conditions,
            limit=self._limit,
            offset=self._offset,
            groupby=groupby,
            orderby=orderby,
        )
        return stm()

    def to_pandas(self, *args, chunked:int=0) -> Union["DataFrame", Iterable["DataFrame"]]:
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
            self.create_df(chunk, names, dtypes)
            for chunk in takewhile(bool, chunkify)
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
        return df

    def to_dict(self, *args) -> Iterable[dict]:
        columns = [f.name for f in dataclasses.fields(self.to_dataclass())]
        for record in self.execute(*args):
            yield dict(zip(columns, record))

    def execute(self, *args):
        return self.trn.execute(self.stm(), args)

    def executemany(self, args):
        return self.trn.executemany(self.stm(), args)

    def __iter__(self):
        return iter(self.execute())


