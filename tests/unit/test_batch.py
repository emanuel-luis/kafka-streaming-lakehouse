import pytest
from hypothesis import given
from hypothesis import strategies as st

from lakestream.consumer.batch import BatchBuffer, Rec, resume_offset, with_backoff


def rec(partition: int, offset: int) -> Rec:
    return Rec("t", partition, offset, 0, None, b"v")


class Clock:
    now = 0.0

    def __call__(self) -> float:
        return self.now


def test_due_by_count() -> None:
    buf = BatchBuffer(3, 5, Clock())
    for i in range(2):
        buf.add(rec(0, i))
    assert not buf.due()
    buf.add(rec(0, 2))
    assert buf.due()


def test_due_by_time_from_first_record() -> None:
    clock = Clock()
    buf = BatchBuffer(100, 5, clock)
    clock.now = 10
    assert not buf.due()  # empty is never due
    buf.add(rec(0, 0))
    clock.now = 14.9
    assert not buf.due()
    clock.now = 15
    assert buf.due()


def test_room() -> None:
    buf = BatchBuffer(3, 5, Clock())
    assert buf.room() == 3
    buf.add(rec(0, 0))
    assert buf.room() == 2
    buf.add(rec(0, 1))
    buf.add(rec(0, 2))
    assert buf.room() == 1  # full: still at least 1


def test_take_resets() -> None:
    clock = Clock()
    buf = BatchBuffer(100, 5, clock)
    buf.add(rec(0, 0))
    assert len(buf.take()) == 1
    assert len(buf) == 0 and not buf.due()
    clock.now = 100
    buf.add(rec(0, 1))
    assert not buf.due()  # the window restarts with the next first record


def test_drop_partitions() -> None:
    buf = BatchBuffer(100, 5, Clock())
    for p, o in [(0, 1), (1, 5), (0, 2), (2, 9)]:
        buf.add(rec(p, o))
    assert buf.drop_partitions({0, 2}) == 3
    assert buf.last_offsets() == {1: 5}
    assert buf.drop_partitions({1}) == 1 and not buf.due()


@given(st.lists(st.tuples(st.integers(0, 5), st.integers(0, 10_000)), max_size=50))
def test_last_offsets_is_per_partition_max(items: list[tuple[int, int]]) -> None:
    buf = BatchBuffer(1000, 5, Clock())
    for p, o in items:
        buf.add(rec(p, o))
    expected: dict[int, int] = {}
    for p, o in items:
        expected[p] = max(expected.get(p, o), o)
    assert buf.last_offsets() == expected


@given(st.integers(-1001, 10**6), st.one_of(st.none(), st.integers(0, 10**6)))
def test_resume_offset(committed: int, last: int | None) -> None:
    got = resume_offset(committed, last)
    assert got == (committed if last is None else max(committed, last + 1))


def test_resume_examples() -> None:
    assert resume_offset(10, 20) == 21  # crash between append and commit
    assert resume_offset(30, 20) == 30  # committed ahead of the table (DLQ-only batch)
    assert resume_offset(-1001, None) == -1001  # nothing anywhere: auto.offset.reset applies
    assert resume_offset(-1001, 4) == 5


def test_with_backoff_retries_then_succeeds() -> None:
    calls: list[int] = []
    sleeps: list[float] = []

    def flaky() -> str:
        calls.append(1)
        if len(calls) < 4:
            raise RuntimeError("boom")
        return "ok"

    assert with_backoff(flaky, 5, sleep=sleeps.append) == "ok"
    assert sleeps == [1, 2, 4]


def test_with_backoff_gives_up() -> None:
    sleeps: list[float] = []

    def always() -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        with_backoff(always, 5, sleep=sleeps.append)
    assert sleeps == [1, 2, 4, 8, 16]
