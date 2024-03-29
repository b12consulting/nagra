from nagra import Statement
from nagra.sexpr import AST


class Delete:
    def __init__(self, table, trn, env):
        self.table = table
        self.env = env
        self.where_asts = []
        self.where_conditions = []
        self.trn = trn

    def where(self, *conditions):
        # TODO clone first
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

    def execute(self, *args):
        return self.trn.execute(self.stm(), args)

    def executemany(self, args):
        return self.trn.executemany(self.stm(), args)

    def __iter__(self):
        return iter(self.execute())
