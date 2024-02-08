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
from datetime import datetime

from nagra import Transaction

__all__ = ["AST"]


UNSET = object()


def list_to_dict(*items):
    it = iter(items)
    return dict(zip(it, it))


class KWargs:
    def __init__(self, *items):
        self.value = list_to_dict(*items)

    def __repr__(self):
        return f'<KWargs "{self.value}">'


class Env:
    def __init__(self, table):
        self.table = table
        self.refs = {}

    def add_ref(self, path):
        *head, name, tail = path
        prefix = tuple([*head, name])
        table_alias = self.refs.get(prefix)
        if not table_alias:
            if len(prefix) >= 2:
                self.add_ref(prefix)
            table_alias = f"{name}_{len(self.refs)}"
            self.refs[prefix] = table_alias
        return f'"{table_alias}"."{tail}"'

    def __repr__(self):
        content = repr(self.refs)
        return f"<Env {self.table.name} {content}>"


class Alias:
    """
    Simple wrapper that combine a value and an alias name
    """

    def __init__(self, value, name):
        self.value = value
        self.name = name


def tokenize(expr):
    lexer = shlex.shlex(expr)
    lexer.wordchars += ".!=<>:{}-"
    for i in lexer:
        yield Token.from_value(i)


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
    builtins = {
        "!=": "{} != {}".format,
        "<": "{} < {}".format,
        "<=": "{} <= {}".format,
        "=": "{} = {}".format,
        ">": "{} > {}".format,
        ">=": "{} >= {}".format,
        "and": lambda *x: " AND ".join(x),
        "or": lambda *x: " OR ".join(x),
        "not": "NOT {}".format,
        "is": "{} is {}".format,
        "in": lambda x, *ys: f"{x} in (%s)" % ", ".join(ys),
        "null": lambda: "NULL",
        "true": lambda: "true",
        "false": lambda: "false",
        "like": "{} LIKE {}".format,
        "ilike": "{} ILIKE {}".format,
        "+" :lambda *xs: (' + '.join("{}" for _ in xs)).format(*xs),
        "-": lambda *xs: (' - '.join("{}" for _ in xs)).format(*xs),
        "*": lambda *xs: (' * '.join("{}" for _ in xs)).format(*xs),
        "/": lambda *xs: (' / '.join("{}" for _ in xs)).format(*xs),
    }

    aggregates = {
        "min": "min({})".format,
        "max": "max({})".format,
        "sum": "sum({})".format,
        "avg": "avg({})".format,
        "every": "every({})".format,
        "count": lambda x="*": f"count({x})",
    }

    def __init__(self, tokens):
        # Auto-wrap sublist into AST
        self.tokens = [
            tk if isinstance(tk, Token) else AST(tk)
            for tk in tokens
        ]

    @classmethod
    def parse(cls, expr):
        res = tokenize(expr)
        tokens = scan(res)[0]
        if isinstance(tokens, Token):
            tokens = [tokens]
        return AST(tokens)

    def chain(self):
        for tk in self.tokens:
            if isinstance (tk, Token):
                yield tk
            else:
                yield from tk.chain()

    def _eval(self, env, top=False):
        head, tail = self.tokens[0], self.tokens[1:]
        args = [tk._eval(env) for tk in tail]
        res = head._eval(env, *args)
        return res if top else "({})".format(res)

    def eval(self, env):
        return self._eval(env, top=True)

    def relations(self):
        for tk in self.chain():
            if tk.is_relation():
                yield tk.value

    def _eval_type(self, env):
        head, tail = self.tokens[0], self.tokens[1:]
        args = [tk._eval_type(env) for tk in tail]
        res = head._eval_type(env, *args)
        return res

    def eval_type(self, env):
        return self._eval_type(env)


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
    def from_value(value):
        if value in AST.builtins:
            return BuiltinToken(value)
        if value in AST.aggregates:
            return AggToken(value)
        if (value[0], value[-1]) == ("{", "}"):
            return ParamToken(value)
        try:
            if '.' in value:
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

    def _eval(self, env, *args):
        return None


class ParamToken(Token):
    "Parameterized Token"

    def __init__(self, value):
        # Remove braces
        self.value = value[1:-1]

    def _eval(self, env, *args):
        placeholder = "%s" if Transaction.flavor == "postgresql" else "?"
        return placeholder


class LitToken(Token):
    "Litteral Token"

    def _eval_type(self, env):
        return self._type


    def _eval(self, env, *args):
        return self.value

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

    def _eval(self, env, *args):
        return f"'{self.value}'"


class VarToken(Token):
    def __init__(self, value):
        self.join_alias = None
        super().__init__(value)

    def is_relation(self):
        return "." in self.value

    def _eval(self, env, *args):
        if self.is_relation():
            self.join_alias = env.add_ref(self.value.split("."))
            return self.join_alias
        return '"{}"."{}"'.format(env.table.name, self.value)

    def _eval_type(self, env):
        if self.value == "id":
            # id is implicit on Table
            return int
        # TODO handle paramtoken here?
        if self.is_relation():
            *head, tail = self.value.split(".")
            ftable, _ = env.table.join_on(tuple(head))
            col_type = ftable.columns[tail]
        else:
            col_type = env.table.columns[self.value]
        match col_type:
            # TODO validate type names int Table.__init__
            case "int" | "bigint":
                return int
            case "varchar":
                return str
            case "float":
                return float
            case "timestamp":
                return datetime
            case "bool":
                return bool
            case "json":
                return str
            case _:
                msg = f"Columns of type {col_type} not supported (for {self.value})"
                raise NotImplementedError(msg)


class OpToken(Token):
    ops = AST.builtins

    def _eval(self, env, *args):
        op = self.ops[self.value]
        return op(*args)


class BuiltinToken(OpToken):
    num_like = "+-*/"
    bool_like = (
        "!=", "<", ">", ">=", "<=", "=",
        "and", "or", "not", "is", "like", "ilike",
    )

    def _eval_type(self, env, *operands):
        # FIXME, probably too basic
        if self.value in self.num_like:
            if any(op == float for op in operands):
                return float
            return int
        elif self.value in self.bool_like:
            return bool
        else:
            return str

class AggToken(OpToken):
    ops = AST.aggregates

    num_like = ["sum", "avg"]
    bool_like = ["every"]

    def _eval_type(self, env, *operands):
        if self.value == "count":
            return int
        if self.value in self.num_like:
            assert operands[0] in (float, int)
            return operands[0]
        if self.value in self.bool_like:
            assert operands[0] == bool
            return bool
        else:
            return operands[0]
