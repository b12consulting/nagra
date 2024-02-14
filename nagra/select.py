import re
import dataclasses

from nagra import Statement
from nagra.mixin import Executable
from nagra.sexpr import Env, AST, AggToken


RE_VALID_IDENTIFIER = re.compile(r"\W|^(?=\d)")

def clean_col(name):
    return RE_VALID_IDENTIFIER.sub('_', name)


class Select(Executable):
    def __init__(self, table, *columns):
        self.table = table
        self.env = Env(table)
        self.columns = columns
        self.columns_ast = [AST.parse(c) for c in columns]
        self.query_columns = [a.eval(self.env) for a in self.columns_ast]
        self.where_asts = []
        self.where_conditions = []
        self._offset = None
        self._limit = None
        self.groupby_ast = None
        self.order_ast = []
        self.order_directions = []
        super().__init__()

    def where(self, *conditions):
        asts = [AST.parse(cond) for cond in conditions]
        self.where_asts += asts
        self.where_conditions += [ast.eval(self.env) for ast in asts]
        return self

    def offset(self, value):
        self._offset = value
        return self

    def limit(self, value):
        self._limit = value
        return self

    def groupby(self, *groups):
        self.groupby_ast = [AST.parse(g) for g in groups]
        return self

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

        self.order_ast = [AST.parse(e) for e in expressions]
        self.order_directions = directions
        return self

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
        groupby_ast = self.groupby_ast or self.infer_groupby()
        groupby = [a.eval(self.env) for a in groupby_ast]

        orderby = [a.eval(self.env) + f" {d}" for a, d in zip(
            self.order_ast,
            self.order_directions,
        )]
        joins = self.table.join(self.env)
        stm = Statement(
            "select",
            table=self.table.name,
            columns=self.query_columns,
            joins=joins,
            conditions=self.where_conditions,
            limit=self._limit,
            offset=self._offset,
            groupby=groupby,
            orderby=orderby,
        )
        return stm()

    def to_pandas(self, rows, aliases=[]):
        """
        Convert the rows into columns and return a df with the
        proper column types, and the given aliases as column names.
        """
        from pandas import DataFrame, Series
        names, dtypes = zip(*(self.dtypes(*aliases)))
        by_col = zip(*rows)
        df = DataFrame()
        for name, dt, col in zip(names, dtypes, by_col):
            # FIXME Series(col, dtype=dt) fail on json cols!
            srs = Series(col)
            if dt == int:
                # Make sure we have no nan for int columns
                srs = srs.fillna(0)
            df[name] = srs.astype(dt)
        return df

    def to_dict(self):
        columns = [f.name for f in dataclasses.fields(self.to_dataclass())]
        return [dict(zip(columns, record)) for record in self]
