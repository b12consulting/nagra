from nagra import View


def test_select_views(transaction, country, population, max_pop, min_pop):
    country.upsert("name").executemany(
        [
            ("Belgium",),
            ("Netherlands",),
        ]
    )

    population.upsert("country.name", "year", "value").executemany(
        [
            ("Belgium", 1970, 10),
            ("Belgium", 1971, 11),
            ("Netherlands", 1970, 12),
            ("Netherlands", 1971, 14),
        ]
    )
    # max_pop was defined by a subselect
    res = list(max_pop.select())
    assert res == [("Belgium", 11), ("Netherlands", 14)]

    res = list(
        max_pop.select("max",).orderby(
            ("country", "desc"),
        )
    )
    assert res == [(14,), (11,)]

    # min_pop was defined by nagra expressions
    res = list(min_pop.select())
    assert res == [("Belgium", 10), ("Netherlands", 12)]

    res = list(
        min_pop.select("min",).orderby(
            "country",
        )
    )
    assert res == [(10,), (12,)]


def test_get_view(min_pop):
    assert min_pop == View.get("min_pop")
