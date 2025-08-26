"""
The `AST` (abstract syntax tree) implement parsing and evaluation
of [s-expressions](https://en.wikipedia.org/wiki/S-expression).

Example:

``` python-console
>>> from nagra.sexpr import AST
>>> AST.parse('(+ 1 1)')
<nagra.sexpr.AST object at 0x7f1bcc5b2fd0>
>>> ast = AST.parse('(+ 1 1)')
>>> ast.eval()
2
```

The `eval` method accepts an `env` parameter, a dictionary used to
evaluate non-litteral tokens:

``` python-console
>>> ast = AST.parse('(+ 1 x)')
>>> ast.eval()
Traceback (most recent call last):
   ...
ValueError: Unexpected token: "x"
>>> ast.eval({'x': 2})
3
```

"""

import shlex
from datetime import date, datetime
from functools import cached_property

from nagra.exceptions import EvalTypeError


DEFAULT_FLAVOR = "postgresql"
__all__ = ["AST"]


def list_to_dict(*items):
    it = iter(items)
    return dict(zip(it, it))


class KWargs:
    def __init__(self, *items):
        self.value = list_to_dict(*items)

    def __repr__(self):
        return f'<KWargs "{self.value}">'


class Alias:
    """
    Simple wrapper that combine a value and an alias name
    """

    def __init__(self, value, name):
        self.value = value
        self.name = name


def tokenize(expr):
    lexer = shlex.shlex(expr)
    lexer.wordchars += ".!=<>:{}-|"
    prev_tk = None
    for i in lexer:
        tk = Token.from_value(i, prev_tk)
        prev_tk = tk
        yield tk


def scan(tokens, end_tk=")"):
    res = []
    for tk in tokens:
        if tk.value == end_tk:
            return res
        elif tk.value == "(":
            res.append(scan(tokens))
        else:
            res.append(tk)

    tail = next(tokens, None)
    if tail:
        raise ValueError(f'Unexpected token: "{tail.value}"')
    return res


class AST:
    # Litterals
    literals = {
        "true",
        "false",
        "null",
        "*",
    }

    # Set of infix operators
    infix = {
        "and",
        "not",
        "is",
        "in",
        "+",
        "-",
        "*",
        "/",
        "||",
        "=",
        "!=",
        "<",
        "<=",
        ">",
        ">=",
        "is",
        "match",
        "ilike",
        "like",
        "||",
        "&",
        "|",
        "#",
        "~",
        "<<",
        ">>",
        "->",
        "->>",
        "#>",
        "#>>",
        "<->",
        "<#>",
        "<=>",
        "<+>",
        "<~>",
        "<%>",
    }
    # Declare special builtins  explicitly
    builtins = {
        "-": (lambda *xs: " - ".join(map(str, xs)) if len(xs) > 1 else f"-{xs[0]}"),
        "not": "NOT {}".format,
        # OR is explicitly declared because of operator precedence
        "or": lambda *x: "(%s)" % (" OR ".join(x)),
        "isnot": "NOT {} IS {}".format,
        "extract": "EXTRACT({} FROM {})".format,
        "in": lambda x, *ys: f"{x} in (%s)" % ", ".join(map(str, ys)),
        # Usefull for IN operation with large number of items (see
        # https://postgres.cz/wiki/PostgreSQL_SQL_Tricks_I#Predicate_IN_optimalization)
        "values": lambda *xs: ("(VALUES %s)" % (
            ",".join("({})" for _ in xs)
        )).format(*xs)
    }

    # Declare aggregate operators
    agg_unary = {
        "min",
        "max",
        "sum",
        "avg",
        "every",
        "count",
        "group_concat",
        "string_agg",
        "array_agg",
        "json_agg",
        "bool_or",
        "bool_and",
        "json_object_agg",
    }
    agg_variadic = {
        "group_concat",
        "string_agg",
    }
    aggregates = agg_unary | agg_variadic

    def __init__(self, tokens):
        # Auto-wrap sublist into AST
        self.tokens = [tk if isinstance(tk, Token) else AST(tk) for tk in tokens]

    @classmethod
    def parse(cls, expr):
        res = tokenize(expr)
        tokens = scan(res)[0]
        if isinstance(tokens, Token):
            tokens = [tokens]
        return AST(tokens)

    def chain(self):
        for tk in self.tokens:
            if isinstance(tk, Token):
                yield tk
            else:
                yield from tk.chain()

    def _eval(self, env, flavor, top=False):
        head, tail = self.tokens[0], self.tokens[1:]
        args = [tk._eval(env, flavor) for tk in tail]
        res = head._eval(env, flavor, *args)
        return res if top else "({})".format(res)

    def eval(self, env, flavor=DEFAULT_FLAVOR):
        return self._eval(env, flavor, top=True)

    def relations(self):
        for tk in self.chain():
            if tk.is_relation():
                yield tk.value

    def _eval_type(self, env):
        # head is always a token
        head, tail = self.tokens[0], self.tokens[1:]
        args = [tk._eval_type(env) for tk in tail]
        res = head._eval_type(env, *args)
        return res

    def eval_type(self, env):
        return self._eval_type(env)

    def is_nullable(self, env):
        return any(
            tk._is_nullable(env)
            for tk in self.chain()
            if not isinstance(tk, (BuiltinToken, ))
        )

    def get_args(self):
        """
        Return token that should be treated as query arguments
        """
        args = list((tk.get_arg() for tk in self.chain()))
        return [a for a in args if a]

    def is_aggregate(self):
        for tk in self.chain():
            if tk.is_aggregate():
                return True
        return False


class Token:
    def __init__(self, value):
        self.value = value

    def is_relation(self):
        return False

    @staticmethod
    def from_value(value, prev_tk):
        # Handle exceptions
        if value == "(":
            return LParen(value)

        # Dot prefix force Vartoken
        if value.startswith("."):
            return VarToken(value.lstrip("."))

        # Literals take precedence over everything else
        if value in AST.literals:
            return LiteralToken(value)

        # First item should be an operator
        if isinstance(prev_tk, LParen):
            if value in AST.literals:
                return LiteralToken(value)
            elif value in AST.aggregates:
                return AggToken(value)
            else:
                return BuiltinToken(value)

        if (value[0], value[-1]) == ("{", "}"):
            return ParamToken(value)
        try:
            if "." in value:
                value = float(value)
                return FloatToken(value)
            else:
                value = int(value)
                return IntToken(value)
        except ValueError:
            pass
        return StrToken(value) if value[0] in "\"'" else VarToken(value)

    def __repr__(self):
        cls = self.__class__.__name__
        return f"<{cls} {self.value}>"

    def get_arg(self):
        return None

    def _eval(self, env, flavor, *args):
        return None


class LParen(Token):
    "Left Parenthesis"


class ParamToken(Token):
    "Parameterized Token"

    def __init__(self, value):
        # Remove braces
        self.value = value[1:-1]
        # TODO self.value placeholder name we should use it to apply
        # param, for example when we do:
        # select.where('(= col {my_input})').execute(my_input=42)

    def _eval(self, env, flavor, *args):
        placeholder = "%s" if flavor == "postgresql" else "?"
        return placeholder


class VarToken(Token):
    def __init__(self, value):
        self.join_alias = None
        super().__init__(value)

    def is_relation(self):
        return "." in self.value

    def _eval(self, env, flavor, *args):
        if self.is_relation():
            self.join_alias = env.add_ref(self.value.split("."))
            return self.join_alias
        return '"{}"."{}"'.format(env.table.name, self.value)

    def _eval_type(self, env):
        # TODO handle paramtoken here?
        if self.is_relation():
            *head, tail = self.value.split(".")
            ftable, _, _ = env.table.join_on(tuple(head), env=env)
            if tail not in ftable.columns and tail == ftable.primary_key:
                # implicit type for pk is int
                # FIXME this would be simpler with id in columns
                return int
            col = ftable.columns[tail]
            return col.python_type()
        elif col := env.table.columns.get(self.value):
            return col.python_type()
        elif self.value == env.table.primary_key:
            # implicit type for pk is integer
            return int
        else:
            raise EvalTypeError(f"Unable to eval type of '{self.value}'")

    def _is_nullable(self, env):
        if self.is_relation():
            value = self.value
            table = env.table
            while "." in value:
                # If any item in the chain is nullable, the all chain is
                head, value = value.split(".", 1)
                if head not in table.not_null:
                    return True
                table = table.schema.get(table.foreign_keys[head])
            # No nullable column in the dotted chain
            return False
        return self.value not in env.table.not_null


class OpToken(Token):
    @cached_property
    def op(self):
        if self.value in AST.builtins:
            return AST.builtins[self.value]
        if self.value in AST.infix:
            return lambda *xs: f" {self.value.upper()} ".join(map(str, xs))
        return lambda *xs: f"{self.value}(%s)" % ", ".join(map(str, xs))

    def _eval(self, env, flavor, *args):
        if self.value in AST.literals:
            return self.value
        return self.op(*args)


class BuiltinToken(OpToken):
    num_like = set(['+', '-', '*', '/'])
    bool_like = set([
        "!=",
        "<",
        ">",
        ">=",
        "<=",
        "=",
        "and",
        "or",
        "not",
        "is",
        "isnot",
        "like",
        "ilike",
        "isfinite",
    ])
    datetime_like = set([
        "clock_timestamp",
        "current_timestamp",
        "date_add",
        "date_bin",
        "date_subtract",
        "date_trunc",
        "localtimestamp",
        "make_timestamp",
        "now",
        "statement_timestamp",
        "to_timestamp",
        "transaction_timestamp",
    ])
    date_like = set([
        "current_date",
        "make_date",
    ])
    float_like = set([
        "date_part",
        "extract",
    ])

    def _eval_type(self, env, *operands):
        # FIXME, probably too basic
        if self.value in self.num_like:
            if all(op == date for op in operands):
                return int
            elif any(op == date for op in operands):
                return date
            elif any(op == float for op in operands):
                return float
            return int
        elif self.value in self.bool_like:
            return bool
        elif self.value in self.float_like:
            return float
        elif self.value in self.datetime_like:
            return datetime
        elif self.value in self.date_like:
            return date
        else:
            return str


class LitToken(Token):
    "Litteral Token"

    def _eval_type(self, env):
        return self._type

    def _eval(self, env, flavor, *args):
        return self.value

    def _is_nullable(self, env):
        return False


class FloatToken(LitToken):
    "Float Token"
    _type = float


class IntToken(LitToken):
    "Integer Token"
    _type = int


class StrToken(LitToken):
    "String Token"

    def __init__(self, value):
        # Remove quotes
        self.value = value[1:-1]

    def _eval_type(self, env):
        return str

    def _eval(self, env, flavor, *args):
        return f"'{self.value}'"


class LiteralToken(OpToken):
    """
    Class for hard-coded litteral token, one of `AST.literals`
    """
    def _eval_type(self, env, *operands):
        if self.value == "null":
            return None
        return bool

    def _is_nullable(self, env, *operands):
        return self.value == "null"


class AggToken(OpToken):
    ops = AST.aggregates

    num_like = ["sum", "avg"]
    bool_like = ["every"]

    def _eval_type(self, env, *operands):
        if self.value == "count":
            return int
        if self.value in self.num_like:
            assert operands[0] in (float, int, list[float], list[int])
            return operands[0]
        if self.value in self.bool_like:
            assert operands[0] == bool
            return bool
        else:
            return operands[0]

    def _is_nullable(self, env, *operands):
        return True
