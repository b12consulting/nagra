from collections.abc import Iterable
from typing import Union, Optional, TYPE_CHECKING


from nagra import Statement
from nagra.exceptions import ValidationError
from nagra.transaction import Transaction
from nagra.writer import WriterMixin


if TYPE_CHECKING:
    from nagra.table import Table


class Update(WriterMixin):
    def __init__(
        self,
        table: "Table",
        *columns: str,
        trn: Transaction,
        lenient: Union[bool, list[str], None] = None,
        where: Iterable[str] = [],
    ):
        self.table = table
        self.columns = list(columns)
        self.lenient = lenient or []
        self._where = list(where)
        self.trn = trn
        super().__init__()

    def clone(
        self,
        trn: Optional["Transaction"] = None,
        where: Iterable[str] = [],
    ):
        """
        Return a copy of update with updated parameters
        """
        trn = trn or self.trn
        where = self._where + list(where)
        cln = Update(
            self.table,
            *self.columns,
            trn=trn,
            lenient=self.lenient,
            where=where,
        )
        return cln

    def where(self, *conditions: str):
        return self.clone(where=conditions)

    def stm(self):
        pk = self.table.primary_key
        condition_key = [pk] if pk in self.groups else self.table.natural_key
        if not all(c in self.groups for c in condition_key):
            msg = "No unique key identifiable in provided columns !"
            # TODO put columns in msg
            raise ValidationError(msg)

        columns = self.groups
        stm = Statement(
            "update",
            self.trn.flavor,
            table=self.table.name,
            columns=columns,
            condition_key=condition_key,
            pk=pk,
        )
        return stm()

    def _exec_args(self, arg_df):
        # We need to reshuffle args values because they must be split
        # into SET and WHERE blocks in the sql statement
        pk = self.table.primary_key
        condition_key = [pk] if pk in self.groups else self.table.natural_key
        set_cols = [c for c in self.groups if c not in condition_key]
        sorted_groups = set_cols + condition_key
        args = zip(*(arg_df[c] for c in sorted_groups))
        return args
