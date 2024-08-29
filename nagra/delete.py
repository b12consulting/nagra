from collections.abc import Iterable
from typing import Optional, TYPE_CHECKING

from nagra import Statement
from nagra.sexpr import AST

if TYPE_CHECKING:
    from nagra.table import Table, Env
    from nagra.transaction import Transaction


class Delete:
    def __init__(
        self, table: "Table", trn: "Transaction", env: "Env", where: Iterable[str] = []
    ):
        self.table = table
        self.trn = trn
        self.env = env
        self._where = list(where)

    def clone(
        self,
        trn: Optional["Transaction"] = None,
        env: Optional["Env"] = None,
        where: Iterable[str] = [],
    ):
        """
        Return a copy of upsert with updated parameters
        """
        trn = trn or self.trn
        where = self._where + list(where)
        cln = Delete(
            self.table,
            env=env or self.env,
            trn=trn,
            where=where,
        )
        return cln

    def where(self, *conditions: str):
        return self.clone(where=conditions)

    def stm(self):
        asts = [AST.parse(cond) for cond in self._where]
        eval_conditions = [ast.eval(self.env, flavor=self.trn.flavor) for ast in asts]
        joins = list(self.table.join(self.env))
        stm = Statement(
            "delete-with-join" if joins else "delete",
            table=self.table.name,
            joins=joins,
            conditions=eval_conditions,
        )
        return stm()

    def __call__(self):
        return self.execute()

    def execute(self, *args):
        return self.trn.execute(self.stm(), args)

    def executemany(self, args):
        return self.trn.executemany(self.stm(), args)

    def __iter__(self):
        return iter(self.execute())
