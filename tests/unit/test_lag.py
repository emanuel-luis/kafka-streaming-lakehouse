from lakestream.ops.lag import caught_up, compute_lag, render


def test_compute_lag() -> None:
    rows = compute_lag([1, 0, 2], {0: 10, 1: -1001}, {0: 10, 1: 4, 2: 0}, {0: "a (m1)"})
    assert [(r.partition, r.committed, r.end, r.lag, r.owner) for r in rows] == [
        (0, 10, 10, 0, "a (m1)"),
        (1, None, 4, 4, None),
        (2, None, 0, 0, None),
    ]
    assert not caught_up(rows)  # partitions 1 and 2 have no committed offset


def test_caught_up() -> None:
    assert caught_up(compute_lag([0, 1], {0: 5, 1: 0}, {0: 5, 1: 0}, {}))
    assert not caught_up(compute_lag([0], {0: 4}, {0: 5}, {}))
    assert not caught_up([])


def test_render_json_and_text() -> None:
    rows = compute_lag([0], {0: 1}, {0: 3}, {})
    assert '"total_lag": 2' in render("g", "t", rows, True)
    assert "total_lag=2" in render("g", "t", rows, False)
