from typing import Optional


from nagra.schema import Schema
from nagra.table import Table
from nagra.exceptions import IncorrectSchema


class View:
    def __init__(
        self,
        name: str,
        view_columns: Optional[dict] = None,
        columns: Optional[dict] = None,
        natural_key: Optional[list[str]] = None,
        foreign_keys: Optional[dict] = None,
        as_select: Optional[str] = None,
        view_select: Optional[str] = None,
        view_where: Optional[str] = None,
        schema: Schema = Schema.default,
    ):
        self.name = name
        self.view_columns = view_columns
        self.as_select = as_select
        self.view_select = view_select
        self.schema = schema

        # Create underlying table
        if not columns:
            if not view_columns:
                msg = (
                    f"Error on view '{name}': one of "
                    "`columns` or `view_columns` must be defined"
                )
                raise IncorrectSchema(msg)

            columns = {}
            select = schema.get(self.view_select).select(*view_columns.values())
            dtypes = select.dtypes(with_optional=False)
            for col_name, (_, dt) in zip(view_columns, dtypes):
                columns[col_name] = dt.__name__

        self.table = Table(
            name,
            columns=columns,
            natural_key=natural_key,
            foreign_keys=foreign_keys,
            is_view=True,
            schema=self.schema,
        )
        # Add view to schema
        self.schema.add_view(self.name, self)

        # We try to quack like a table
        self.columns = self.table.columns

    def select(self, *columns, trn=None):
        return self.table.select(*columns, trn=trn)

    def view_def(self):
        if self.as_select:
            return self.as_select
        # generate a select from the referenced table
        stm = (
            self.schema.get(self.view_select)
            .select(*self.view_columns.values())
            .aliases(*self.view_columns.keys())
            .stm()
        )
        return stm.rstrip(";")

    @classmethod
    def get(self, name: str, schema: Schema = Schema.default):
        """
        Return view object for the given `name`
        """
        return schema.views.get(name)
