"""
.. include:: ../README.md
"""

from importlib.metadata import version

from .statement import Statement
from .schema import Schema
from .table import Table
from .view import View
from .transaction import Transaction


__version__ = version("nagra")
