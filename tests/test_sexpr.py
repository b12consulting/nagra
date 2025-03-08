from nagra.sexpr import AST
from nagra.table import Table, Env


def test_sexpr():
    # Simple dot reference
    expr = "ham.spam"
    ast = AST.parse(expr)
    assert str(ast.tokens) == "[<VarToken ham.spam>]"

    # With int literal
    expr = "(= ham.spam 1)"
    ast = AST.parse(expr)
    assert str(ast.tokens) == "[<BuiltinToken =>, <VarToken ham.spam>, <IntToken 1>]"

    # With string literal
    expr = "(= ham.spam 'one')"
    ast = AST.parse(expr)
    assert str(ast.tokens) == "[<BuiltinToken =>, <VarToken ham.spam>, <StrToken one>]"

    # With placeholder
    expr = "(= ham.spam {})"
    ast = AST.parse(expr)
    assert str(ast.tokens) == "[<BuiltinToken =>, <VarToken ham.spam>, <ParamToken >]"

    # With string operator
    expr = "(|| 'one' 'two')"
    ast = AST.parse(expr)
    assert str(ast.tokens) == "[<BuiltinToken ||>, <StrToken one>, <StrToken two>]"

    # With variable clashing with an aggregate
    expr = "(max min)"
    ast = AST.parse(expr)
    assert str(ast.tokens) == "[<AggToken max>, <VarToken min>]"

    # With lone aggregate
    expr = "(count)"
    ast = AST.parse(expr)
    assert str(ast.tokens) == "[<AggToken count>]"

    # With an operator sign as variable
    expr = "(1 + 1)"
    ast = AST.parse(expr)
    assert str(ast.tokens) == "[<IntToken 1>, <VarToken +>, <IntToken 1>]"

    # With litterals
    expr = "(is null true)"
    ast = AST.parse(expr)
    assert (
        str(ast.tokens)
        == "[<BuiltinToken is>, <LiteralToken null>, <LiteralToken true>]"
    )

    # Use dot prefix to escape literal
    expr = "(is null .true))"
    ast = AST.parse(expr)
    assert (
        str(ast.tokens) == "[<BuiltinToken is>, <LiteralToken null>, <VarToken true>]"
    )

    # Compare litterals
    expr = "(!= true false)"
    ast = AST.parse(expr)
    assert (
        str(ast.tokens)
        == "[<BuiltinToken !=>, <LiteralToken true>, <LiteralToken false>]"
    )


def test_find_relations():
    expr = "(= ham.spam foo.bar)"
    ast = AST.parse(expr)
    assert list(ast.relations()) == ["ham.spam", "foo.bar"]


def test_simple_eval():
    table = Table("spam", {"a": "bool"})
    expr = "a"
    ast = AST.parse(expr)
    env = Env(table)
    assert ast.eval(env) == '"spam"."a"'

    expr = "(= a 1)"
    ast = AST.parse(expr)
    env = Env(table)
    assert ast.eval(env) == '"spam"."a" = 1'

    expr = "(= a (= 1 1))"
    ast = AST.parse(expr)
    env = Env(table)
    assert ast.eval(env) == '"spam"."a" = (1 = 1)'

    # AND and OR, OR add extra parents in evaled form
    expr = "(and true true)"
    ast = AST.parse(expr)
    env = Env(table)
    assert ast.eval(env) == 'true AND true'

    expr = "(or true true)"
    ast = AST.parse(expr)
    env = Env(table)
    assert ast.eval(env) == '(true OR true)'

    expr = "(or (and true false) true)"
    ast = AST.parse(expr)
    env = Env(table)
    assert ast.eval(env) == '((true AND false) OR true)'


def test_join_eval(person):
    # Unique  join
    env = Env(table=person)
    expr = "(and (= parent.name 'Roger')"
    ast = AST.parse(expr)
    res = ast.eval(env)
    assert env.refs == {
        ("parent",): "parent_0",
    }
    assert res == """("parent_0"."name" = 'Roger')"""

    # Double join with two different depth
    env = Env(table=person)
    expr = "(and (= parent.name 'Roger') (= parent.parent.name 'George')"
    ast = AST.parse(expr)
    res = ast.eval(env)
    assert env.refs == {
        ("parent",): "parent_0",
        ("parent", "parent"): "parent_1",
    }
    assert res == """("parent_0"."name" = 'Roger') AND ("parent_1"."name" = 'George')"""

    # Double join with same depth
    env = Env(table=person)
    expr = "(and (= parent.name 'Roger') (= parent.id 1)"
    ast = AST.parse(expr)
    res = ast.eval(env)
    assert env.refs == {
        ("parent",): "parent_0",
    }
    assert res == """("parent_0"."name" = 'Roger') AND ("parent_0"."id" = 1)"""
