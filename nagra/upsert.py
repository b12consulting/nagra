from typing import Union, Optional, TYPE_CHECKING
from collections.abc import Iterable
from dataclasses import dataclass

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None

from nagra import Statement, Schema
from nagra.transaction import Transaction
from nagra.writer import WriterMixin
from nagra.utils import snake_to_pascal, get_table_from_dataclass, iter_dataclass_cols

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
        check: Iterable[str] = [],
    ):
        self.table = table
        self.columns = [c.lstrip(".") for c in columns]
        self._insert_only = insert_only
        self.lenient = lenient or []
        self._check = list(check)
        self.trn = trn
        self.env = env
        super().__init__()

    def clone(
        self,
        trn: Optional["Transaction"] = None,
        insert_only: Optional[bool] = None,
        check: Iterable[str] = [],
    ):
        """
        Return a copy of upsert with updated parameters
        """
        trn = trn or self.trn
        insert_only = self._insert_only if insert_only is None else insert_only
        check = self._check + list(check)
        cln = Upsert(
            self.table,
            *self.columns,
            trn=trn,
            env=self.env.clone(),
            lenient=self.lenient,
            insert_only=insert_only,
            check=check,
        )
        return cln

    def insert_only(self):
        return self.clone(insert_only=True)

    def check(self, *conditions: str):
        return self.clone(check=conditions)

    def stm(self):
        pk = self.table.primary_key
        with_pk = pk in self.groups
        conflict_key = [pk] if with_pk else self.table.natural_key
        columns = self.groups
        do_update = False if self._insert_only else len(columns) > len(conflict_key)
        set_identity = (
            self.trn.flavor == "mssql"
            and with_pk
            and self.table.primary_key_is_identity
        )
        stm = Statement(
            "upsert",
            self.trn.flavor,
            table=self.table.name,
            columns=columns,
            conflict_key=conflict_key,
            do_update=do_update,
            returning=[pk] if pk else self.table.natural_key,
            set_identity=set_identity,
        )
        return stm()

    def _exec_args(self, arg_df):
        args = zip(*(arg_df[c] for c in self.groups))
        return args

    def resolve(self, column: str, *values: list[any]):
        rows = list(zip(*values))
        yield from self._resolve(column, rows)

    @staticmethod
    def from_dataclass(cls: dataclass, schema: Schema = Schema.default) -> "Upsert":
        # TODO add from_pydantic
        table = get_table_from_dataclass(cls, schema)
        cols = list(iter_dataclass_cols(cls))
        return table.upsert(*cols)
