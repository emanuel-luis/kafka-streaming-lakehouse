"""Pure batching helpers: the record accumulator, resume offsets and bounded retry."""

import time
from collections.abc import Callable, Collection
from dataclasses import dataclass

BACKOFF_SECONDS = (1, 2, 4, 8, 16)


@dataclass(frozen=True)
class Rec:
    """One consumed Kafka record, detached from the client's Message object."""

    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: bytes | None
    value: bytes | None


class BatchBuffer:
    """Collects records until `max_messages` or until `max_seconds` after the first one."""

    def __init__(
        self,
        max_messages: int,
        max_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.max_messages = max_messages
        self.max_seconds = max_seconds
        self._clock = clock
        self._recs: list[Rec] = []
        self._started: float | None = None

    def __len__(self) -> int:
        return len(self._recs)

    def add(self, rec: Rec) -> None:
        if not self._recs:
            self._started = self._clock()
        self._recs.append(rec)

    def due(self) -> bool:
        if not self._recs or self._started is None:
            return False
        return (
            len(self._recs) >= self.max_messages
            or self._clock() - self._started >= self.max_seconds
        )

    def last_offsets(self) -> dict[int, int]:
        """Inclusive last offset per partition (records of one partition arrive in order)."""
        out: dict[int, int] = {}
        for r in self._recs:
            out[r.partition] = max(out.get(r.partition, r.offset), r.offset)
        return out

    def drop_partitions(self, partitions: Collection[int]) -> int:
        """Forget the buffered records of revoked partitions; returns how many were dropped."""
        gone = set(partitions)
        before = len(self._recs)
        self._recs = [r for r in self._recs if r.partition not in gone]
        if not self._recs:
            self._started = None
        return before - len(self._recs)

    def take(self) -> list[Rec]:
        recs, self._recs, self._started = self._recs, [], None
        return recs


def resume_offset(committed: int, last_written: int | None) -> int:
    """Where a partition resumes: max(committed, last_written + 1).

    `committed` may be a negative librdkafka sentinel (no commit yet); it then loses to any real
    offset, and with nothing written the sentinel is kept so `auto.offset.reset` applies.
    """
    return committed if last_written is None else max(committed, last_written + 1)


def with_backoff[T](
    fn: Callable[[], T],
    retries: int,
    on_error: Callable[[Exception, int, float], None] = lambda *_: None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call `fn`, retrying any Exception up to `retries` times with the 1, 2, 4, 8, 16 s backoff."""
    for attempt in range(retries + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == retries:
                raise
            delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            on_error(exc, attempt + 1, delay)
            sleep(delay)
    raise AssertionError("unreachable")
