from typing import Union, Optional, TYPE_CHECKING
from collections.abc import Iterable

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None

from nagra import Statement
from nagra.transaction import Transaction
from nagra.writer import WriterMixin
from nagra.utils import UNSET
from nagra.sexpr import AST

if TYPE_CHECKING:
    from nagra.table import Table, Env


class Upsert(WriterMixin):
    def __init__(
        self,
        table: "Table",
        *columns: str,
        trn: Transaction,
        env: "Env",
        lenient: Union[bool, list[str], None] = None,
        insert_only: bool = False,
        where: Iterable[str] = [],
    ):
        self.table = table
        self.columns = [c.lstrip(".") for c in columns]
        self._insert_only = insert_only
        self.lenient = lenient or []
        self._where = list(where)
        self.trn = trn
        self.env = env
        super().__init__()

    def clone(
        self,
        trn: Optional["Transaction"] = None,
        insert_only: Optional[bool] = None,
        where: Iterable[str] = [],
    ):
        """
        Return a copy of upsert with updated parameters
        """
        trn = trn or self.trn
        insert_only = self._insert_only if insert_only is None else insert_only
        where = self._where + list(where)
        cln = Upsert(
            self.table,
            *self.columns,
            trn=trn,
            env=self.env.clone(),
            lenient=self.lenient,
            insert_only=insert_only,
            where=where,
        )
        return cln

    def insert_only(self):
        return self.clone(insert_only=True)

    def where(self, *conditions: str):
        return self.clone(where=conditions)

    def stm(self):
        pk = self.table.primary_key
        conflict_key = [pk] if pk in self.groups else self.table.natural_key
        columns = self.groups
        do_update = False if self._insert_only else len(columns) > len(conflict_key)
        stm = Statement(
            "upsert",
            self.trn.flavor,
            table=self.table.name,
            columns=columns,
            conflict_key=conflict_key,
            do_update=do_update,
            pk=pk if pk is not UNSET else None,
        )
        return stm()

    def _exec_args(self, arg_df):
        args = zip(*(arg_df[c] for c in self.groups))
        return args
