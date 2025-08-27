from importlib.metadata import version

from .statement import Statement
from .table import Table
from .view import View
from .schema import Schema
from .transaction import Transaction


__version__ = version("nagra")
