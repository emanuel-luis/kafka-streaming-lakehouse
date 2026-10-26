from collections import defaultdict
from datetime import datetime, timedelta
from itertools import pairwise

import pytest
from hypothesis import given
from hypothesis import strategies as st

from lakestream.contracts import validate
from lakestream.producer.generator import GenParams, generate
from lakestream.producer.model import Event

params = st.builds(
    GenParams,
    sessions=st.integers(0, 60),
    users=st.integers(1, 30),
    seed=st.integers(0, 2**32),
    span_minutes=st.integers(1, 150),
)


@given(params)
def test_invariants(p: GenParams) -> None:
    events = generate(p)
    end = p.base_time + timedelta(minutes=p.span_minutes)
    assert all(p.base_time <= e.event_time < end for e in events)
    assert [e.event_time for e in events] == sorted(e.event_time for e in events)
    assert all(validate(e.as_record()) is None for e in events)
    sessions: dict[str, list[Event]] = defaultdict(list)
    for e in events:
        sessions[e.session_id].append(e)
    assert len(sessions) == p.sessions
    for evs in sessions.values():
        times = [e.event_time for e in evs]
        assert all(b - a <= timedelta(minutes=5) for a, b in pairwise(times))
    per_user: dict[str, list[tuple[datetime, datetime]]] = defaultdict(list)
    for evs in sessions.values():
        per_user[evs[0].user_id].append((evs[0].event_time, evs[-1].event_time))
    for spans in per_user.values():
        spans.sort()
        assert all(b[0] - a[1] >= timedelta(minutes=45) for a, b in pairwise(spans))


@given(params)
def test_deterministic(p: GenParams) -> None:
    assert generate(p) == generate(p)


def test_default_run_shape() -> None:
    events = generate(GenParams())
    assert {e.event_type for e in events} == {"page_view", "add_to_cart", "order_placed", "payment"}
    assert len({e.event_id for e in events}) == len(events)


@pytest.mark.parametrize("span", [0, 151])
def test_span_limit(span: int) -> None:
    with pytest.raises(ValueError, match="span-minutes"):
        GenParams(span_minutes=span)
