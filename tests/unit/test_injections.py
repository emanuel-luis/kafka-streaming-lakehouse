import random

from hypothesis import given
from hypothesis import strategies as st

from lakestream.contracts import REJECT_REASONS, validate
from lakestream.producer.generator import (
    GenParams,
    InjectParams,
    bad_magic_frame,
    generate,
    make_invalid,
    plan_sends,
    truncate_frame,
)
from lakestream.producer.manifest import build_manifest
from lakestream.producer.model import Event
from lakestream.runmeta import utc_now

gens = st.builds(
    GenParams, sessions=st.integers(1, 40), users=st.integers(1, 20), seed=st.integers(0, 10**6)
)
injs = st.builds(
    InjectParams,
    duplicate_rate=st.floats(0, 0.5),
    late_rate=st.floats(0, 0.5),
    late_max_seconds=st.integers(0, 600),
    malformed=st.integers(0, 12),
    invalid=st.integers(0, 21),
)


@given(gens, injs)
def test_plan_counts_and_disjoint(p: GenParams, inj: InjectParams) -> None:
    plan = plan_sends(generate(p), p, inj)
    assert not set(plan.duplicate_ids) & set(plan.late_ids)
    kinds = [i.kind for i in plan.items]
    assert kinds.count("duplicate") == len(plan.duplicate_ids)
    assert kinds.count("late") == len(plan.late_ids)
    assert kinds.count("invalid") == inj.invalid == sum(plan.invalid_by_reason.values())
    assert kinds.count("bad_magic") + kinds.count("truncated") == inj.malformed
    assert kinds.count("clean") + kinds.count("late") == len(plan.clean)
    expected = len(plan.clean) + len(plan.duplicate_ids) + inj.invalid + inj.malformed
    assert len(plan.items) == expected
    first = {i.event.event_id: n for n, i in enumerate(plan.items) if i.kind in ("clean", "late")}
    for n, i in enumerate(plan.items):
        if i.kind == "duplicate":
            assert first[i.event.event_id] < n


@given(gens, injs)
def test_manifest_arithmetic(p: GenParams, inj: InjectParams) -> None:
    plan = plan_sends(generate(p), p, inj)
    now = utc_now()
    m = build_manifest(
        plan=plan,
        run_id="r",
        seed=p.seed,
        schema_version=1,
        base_time=p.base_time,
        span_minutes=p.span_minutes,
        sessions=p.sessions,
        started_at=now,
        finished_at=now,
        delivered=len(plan.items),
        failed=0,
    )
    prod = m["produced"]
    assert (
        prod["total"]
        == m["clean_events"] + m["duplicates"] + m["invalid"]["total"] + (prod["malformed"])
    )
    assert prod["avro"] == prod["total"] - prod["malformed"]
    assert sum(c for _, _, c in m["minute_counts"]) == m["clean_events"]
    assert sum(m["events_by_type"].values()) == m["clean_events"]


def test_each_invalid_trips_exactly_its_reason() -> None:
    p = GenParams()
    rng = random.Random(1)
    for _ in range(20):
        for reason in REJECT_REASONS:
            assert validate(make_invalid(rng, p, reason).as_record()) == reason


def test_invalid_cycle_covers_all_reasons() -> None:
    p = GenParams()
    plan = plan_sends(generate(p), p, InjectParams(invalid=14))
    assert plan.invalid_by_reason == dict.fromkeys(REJECT_REASONS, 2)
    found = [validate(i.event.as_record()) for i in plan.items if i.kind == "invalid"]
    assert sorted(r or "" for r in found) == sorted(REJECT_REASONS * 2)


def test_malformed_frames() -> None:
    ev = generate(GenParams(sessions=1))[0]
    assert bad_magic_frame(ev)[0] != 0
    frame = b"\x00\x00\x00\x00\x01" + bytes(range(40))
    cut = truncate_frame(frame)
    assert cut[:5] == frame[:5]
    assert 5 < len(cut) < len(frame)


def _send_ms(plan_item_event: Event) -> int:
    return int(plan_item_event.event_time.timestamp() * 1000)


@given(gens, injs)
def test_late_events_are_overtaken_within_bound(p: GenParams, inj: InjectParams) -> None:
    events = generate(p)
    plan = plan_sends(events, p, inj)
    sent = [i.event for i in plan.items if i.kind in ("clean", "late")]
    pos = {e.event_id: n for n, e in enumerate(sent)}
    late = set(plan.late_ids)
    assert len(late) == len(plan.late_ids)
    assert not late & set(plan.duplicate_ids)
    if inj.late_max_seconds == 0:
        assert not late
    for e in events:
        if e.event_id not in late:
            continue
        assert any(
            o.user_id == e.user_id
            and o.event_time > e.event_time
            and pos[o.event_id] < pos[e.event_id]
            for o in events
        )


@given(gens, injs)
def test_late_delay_at_most_max(p: GenParams, inj: InjectParams) -> None:
    # re-derive send times: a late event i is sent at t_j + extra, so it sorts after j and the
    # delay bound shows up as: no event of another user sent before t_i + max can be later than i
    events = generate(p)
    plan = plan_sends(events, p, inj)
    sent = [i.event for i in plan.items if i.kind in ("clean", "late")]
    pos = {e.event_id: n for n, e in enumerate(sent)}
    max_ms = inj.late_max_seconds * 1000
    for eid in plan.late_ids:
        e = next(x for x in events if x.event_id == eid)
        # every event whose time is >= t_i + max is sent after i (delay <= max)
        for o in events:
            if _send_ms(o) - _send_ms(e) > max_ms:
                assert pos[o.event_id] > pos[eid]


@given(gens, injs)
def test_plan_is_deterministic(p: GenParams, inj: InjectParams) -> None:
    events = generate(p)
    assert plan_sends(events, p, inj) == plan_sends(events, p, inj)
