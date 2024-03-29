import re
import dataclasses
from datetime import datetime

from nagra import Statement, Transaction
from nagra.sexpr import AST, AggToken


RE_VALID_IDENTIFIER = re.compile(r"\W|^(?=\d)")

def clean_col(name):
    return RE_VALID_IDENTIFIER.sub('_', name)


class Select:
    def __init__(self, table, *columns, env, transaction=None):
        self.table = table
        self.env = env #Env(table)
        self.where_asts = tuple()
        self._offset = None
        self._limit = None
        self.groupby_ast = tuple()
        self.order_ast = tuple()
        self.order_directions = tuple()
        self.columns = tuple()
        self.columns_ast = tuple()
        self.query_columns = tuple()
        self.transaction = transaction
        self._add_columns(columns)

    def _add_columns(self, columns):
        self.columns += columns
        self.columns_ast += tuple(AST.parse(c) for c in columns)
        self.query_columns += tuple(a.eval(self.env) for a in self.columns_ast)

    def clone(self):
        """
        Return a copy of select with updated parameters
        """
        cln = Select(self.table, *self.columns, env=self.env.clone())
        cln.where_asts = self.where_asts
        cln.groupby_ast = self.groupby_ast
        cln.order_ast = self.order_ast
        cln.order_directions  = self.order_directions
        return cln

    def where(self, *conditions):
        cln = self.clone()
        cln.where_asts += tuple(AST.parse(cond) for cond in conditions)
        return cln

    def select(self, *columns):
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

    def groupby(self, *groups):
        cln = self.clone()
        cln.groupby_ast += tuple(AST.parse(g) for g in groups)
        return cln

    def orderby(self, *orders):
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

    def to_dataclass(self, *aliases):
        # TODO return nullable union type if a column is not required
        return dataclasses.make_dataclass(
            self.table.name,
            fields=[(clean_col(c), d) for c, d in self.dtypes(*aliases)]
        )

    def dtypes(self, *aliases):
        fields = []
        if aliases:
            assert len(aliases) == len(self.columns)
            col_names = aliases
        else:
            col_names = self.columns
        for col_name, col_ast in zip(col_names, self.columns_ast):
            col_type = col_ast.eval_type(self.env)
            fields.append((col_name, col_type))
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
        where_conditions = [ast.eval(self.env) for ast in self.where_asts]
        # Eval Groupby
        groupby_ast = self.groupby_ast or self.infer_groupby()
        groupby = [a.eval(self.env) for a in groupby_ast]
        # Eval Oder by
        orderby = [a.eval(self.env) + f" {d}" for a, d in zip(
            self.order_ast,
            self.order_directions,
        )]
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

    def to_pandas(self, *args):
        """
        Convert the rows into columns and return a df with the
        proper column types, and the given aliases as column names.
        """
        from pandas import DataFrame, Series
        names, dtypes = zip(*(self.dtypes()))
        by_col = zip(*self.execute(*args))
        df = DataFrame()
        for name, dt, col in zip(names, dtypes, by_col):
            # FIXME Series(col, dtype=dt) fail on json cols!
            srs = Series(col)
            if dt == int:
                # Make sure we have no nan for int columns
                srs = srs.fillna(0)
            elif dt == datetime:
                dt = str
            df[name] = srs.astype(dt)
        return df

    def to_dict(self):
        columns = [f.name for f in dataclasses.fields(self.to_dataclass())]
        return [dict(zip(columns, record)) for record in self]

    def execute(self, *args):
        transaction = self.transaction or Transaction.current
        return transaction.execute(self.stm(), args)

    def executemany(self, args):
        transaction = self.transaction or Transaction.current
        return transaction.executemany(self.stm(), args)

    def __iter__(self):
        return iter(self.execute())
