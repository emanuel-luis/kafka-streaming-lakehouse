"""Read the Iceberg source position from a Structured Streaming checkpoint. No pyspark here.

Format assumptions (Spark 4.1, Iceberg 1.12), checked by the unit tests:

- `<checkpoint>/offsets/N` is written before batch N runs and holds the END offset of batch N:
  line 1 `v1`, line 2 the batch metadata JSON, then one line per source (JSON, or `-`).
- `<checkpoint>/commits/N` is written after batch N committed to the sink. After a crash, the
  restart re-runs batch N from the end offset of the last committed batch, so that offset (not
  the newest `offsets/N`) is the oldest position the stream may still read.
- Iceberg's source offset is StreamingOffset JSON:
  `{"version":1,"snapshot_id":N,"position":N,"scan_all_files":false}`; Iceberg 1.12 writes the
  id as a number, and a string is accepted too.
- `<checkpoint>/sources/0/offsets/0` is Iceberg's initial offset, written once when the query
  first starts: the bare StreamingOffset JSON of the oldest snapshot it will read. Iceberg 1.12's
  `prepareForTriggerAvailableNow` plans from this offset on every available-now run (checked in
  the bytecode of iceberg-spark-runtime-4.1_2.13 1.12.0), so expiring that snapshot breaks every
  later available-now run with "Cannot load current offset at snapshot ..." (decision 24).
"""

import json
from datetime import datetime
from pathlib import Path

OFFSET_LOG_VERSION = "v1"


def parse_offset_file(text: str) -> int | None:
    """The `snapshot_id` of the Iceberg source in one offset-log file, or None if it has none."""
    lines = text.splitlines()
    if not lines:
        return None
    if lines[0].strip() != OFFSET_LOG_VERSION:
        raise ValueError(f"unsupported offset log version {lines[0]!r}, expected 'v1'")
    return _first_snapshot_id(lines[2:])


def _first_snapshot_id(lines: list[str]) -> int | None:
    for line in lines:
        try:
            offset = json.loads(line)
        except ValueError:
            continue
        if isinstance(offset, dict) and offset.get("snapshot_id") is not None:
            return int(offset["snapshot_id"])
    return None


def initial_snapshot_id(checkpoint_dir: Path) -> int | None:
    """Snapshot id of Iceberg's initial offset (`sources/0/offsets/0`), or None when absent."""
    path = checkpoint_dir / "sources" / "0" / "offsets" / "0"
    if not path.is_file():
        return None
    return _first_snapshot_id(path.read_text(encoding="utf-8").splitlines())


def _highest_batch(log_dir: Path) -> int | None:
    batches = [int(p.name) for p in log_dir.glob("*") if p.name.isdigit()]
    return max(batches, default=None)


def committed_snapshot_id(checkpoint_dir: Path) -> int | None:
    """Snapshot id in `offsets/<highest batch under commits/>`, or None without a committed batch."""
    batch = _highest_batch(checkpoint_dir / "commits")
    if batch is None:
        return None
    offsets = checkpoint_dir / "offsets" / str(batch)
    if not offsets.is_file():
        return None
    return parse_offset_file(offsets.read_text(encoding="utf-8"))


def protected_snapshot_ids(checkpoint_dir: Path) -> list[int] | None:
    """Snapshots of the stream's source that expiry must keep: initial and last committed.

    None when either is unknown (no checkpoint yet, or a layout these assumptions do not cover):
    the caller must then not expire the source table at all. A negative id is Iceberg's start
    offset (the query began on an empty table): it pins no snapshot, so it is left out.
    """
    initial, committed = initial_snapshot_id(checkpoint_dir), committed_snapshot_id(checkpoint_dir)
    if initial is None or committed is None:
        return None
    return [s for s in (initial, committed) if s >= 0]


def clamp_older_than(requested: datetime, resume_commits: list[datetime | None]) -> datetime | None:
    """The bronze expiry cutoff, or None when bronze must not be expired.

    `resume_commits` holds the commit time of the snapshot each silver query resumes from (None
    when unknown). Expiry removes snapshots strictly older than the cutoff, so a cutoff at a
    resume snapshot's commit time keeps that snapshot and everything after it.
    """
    if not resume_commits or any(c is None for c in resume_commits):
        return None
    return min([requested, *(c for c in resume_commits if c is not None)])
