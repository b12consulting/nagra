from itertools import islice
from typing import Iterable, Union, TYPE_CHECKING

from nagra.transaction import Transaction


if TYPE_CHECKING:
    from nagra.table import Table


def copy_from(
    table: "Table",
    rows: Iterable[tuple],
    trn: Transaction,
    lenient: Union[bool, list[str], None] = None,
):
    """
    Populate table with a COPY FROM statement.
    """

    if trn.flavor == "sqlite":
        raise NotImplementedError("COPY FROM not available for sqlite")

    stm = f'COPY "{table.name}" FROM STDIN'
    cursor = trn.connection.cursor()
    it = iter(rows)
    with cursor.copy(stm) as copy:
        while True:
            chunk = list(islice(it, 1000))
            if not chunk:
                break
            content = serialize_chunk(chunk, table.columns)
            copy.write(content)


def serialize_chunk(rows, columns):
    row_strs = ("\t".join(stringify(v) for v in row) for row in rows)
    return "\n".join(row_strs) + "\n"


def stringify(value):
    """
    Convert value to string for COPY FROM
    """
    if isinstance(value, str):
        return f'"{value}"'
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif value is None:
        return "\\N"
    else:
        return str(value)
