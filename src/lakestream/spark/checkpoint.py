"""Read the Iceberg source position from a Structured Streaming checkpoint. No pyspark here.

Format assumptions (Spark 4.1, Iceberg 1.12), checked by the unit tests:

- `<checkpoint>/offsets/N` is written before batch N runs and holds the END offset of batch N:
  line 1 `v1`, line 2 the batch metadata JSON, then one line per source (JSON, or `-`).
- `<checkpoint>/commits/N` is written after batch N committed to the sink. After a crash, the
  restart re-runs batch N from the end offset of the last committed batch, so that offset (not
  the newest `offsets/N`) is the oldest position the stream may still read.
- Iceberg's source offset is StreamingOffset JSON:
  `{"version":1,"snapshot_id":N,"position":N,"scan_all_files":false}`; Iceberg writes the id as
  a string, and a number is accepted too.
"""

import json
from pathlib import Path

OFFSET_LOG_VERSION = "v1"


def parse_offset_file(text: str) -> int | None:
    """The `snapshot_id` of the Iceberg source in one offset-log file, or None if it has none."""
    lines = text.splitlines()
    if not lines:
        return None
    if lines[0].strip() != OFFSET_LOG_VERSION:
        raise ValueError(f"unsupported offset log version {lines[0]!r}, expected 'v1'")
    for line in lines[2:]:
        try:
            offset = json.loads(line)
        except ValueError:
            continue
        if isinstance(offset, dict) and offset.get("snapshot_id") is not None:
            return int(offset["snapshot_id"])
    return None


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
