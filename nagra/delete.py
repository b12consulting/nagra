from nagra import Statement
from nagra.mixin import Executable
from nagra.sexpr import Env, AST


class Delete(Executable):
    def __init__(self, table):
        self.table = table
        self.env = Env(table)
        self.where_asts = []
        self.where_conditions = []
        super().__init__()

    def where(self, *conditions):
        asts = [AST.parse(cond) for cond in conditions]
        self.where_asts += asts
        self.where_conditions += [ast.eval(self.env) for ast in asts]
        return self

    def stm(self):
        joins = list(self.table.join(self.env))
        stm = Statement(
            "delete-with-join" if joins else "delete",
            table=self.table.name,
            joins=joins,
            conditions=self.where_conditions,
        )
        return stm()

    def args(self):
        res = []
        for ast in self.where_asts:
            res += ast.get_args()
        return res

    def __call__(self):
        return self.execute()
