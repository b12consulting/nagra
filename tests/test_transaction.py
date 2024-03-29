import pytest

from nagra.transaction import dummy_transaction
from nagra.exceptions import NoActiveTransaction

def test_dummy_transaction():

    with pytest.raises(NoActiveTransaction):
        dummy_transaction.execute("SELECT 1")

    with pytest.raises(NoActiveTransaction):
        dummy_transaction.executemany("SELECT 1")
