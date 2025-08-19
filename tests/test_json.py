

def test_adpaters(transaction, kitchensink):
    upsert = kitchensink.upsert("varchar", "json")
    upsert.execute("one", [{"foo": "bar"}])
    upsert.execute("two", {"foo": "bar"})

    row, = kitchensink.select("varchar", "json").where("(= varchar  'one')")
    assert row == ('one', [{'foo': 'bar'}])
