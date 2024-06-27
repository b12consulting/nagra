from datetime import datetime


def test_simple_update_by_id(transaction, person):
    # First upsert some values
    upsert = person.upsert("name")
    new_id = upsert.execute("Bob")

    # Update by id
    update = person.update("id", "name")
    new_id_copy = update.execute(new_id, "BOB")
    assert new_id_copy == new_id

    # Test update is successful
    row, = person.select("id", "name")
    assert row == (new_id, "BOB")


def test_simple_update_by_nk(transaction, temperature):
    # First upsert some values
    upsert = temperature.upsert("timestamp", "city", "value")
    new_id = upsert.execute("2024-06-27 17:52:00", "Brussels", 27)

    # Update by nk
    update = temperature.update("timestamp", "city", "value")
    new_id_copy = update.execute("2024-06-27 17:52:00", "Brussels", 28)
    assert new_id_copy == new_id

    # Test update is successful
    row, = temperature.select("timestamp", "city", "value")
    ts = "2024-06-27 17:52:00"
    if transaction.flavor != "sqlite":
        ts = datetime.fromisoformat(ts)
    assert row == (ts, "Brussels", 28)
