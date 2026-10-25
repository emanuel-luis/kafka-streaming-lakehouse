"""Read the Iceberg source position from a Structured Streaming checkpoint. No pyspark here."""

import json
from pathlib import Path


def parse_offset_file(text: str) -> int | None:
    """The `snapshot_id` of the Iceberg source in one offset-log file, or None.

    Layout: line 1 the version (`v1`), line 2 the batch metadata JSON, then one line per source
    (a JSON offset, or `-` when the source had none).
    """
    for line in text.splitlines()[2:]:
        try:
            offset = json.loads(line)
        except ValueError:
            continue
        if isinstance(offset, dict) and offset.get("snapshot_id") is not None:
            return int(offset["snapshot_id"])
    return None


def latest_snapshot_id(checkpoint_dir: Path) -> int | None:
    """`parse_offset_file` of the highest-numbered file under `<checkpoint_dir>/offsets`."""
    batches = [p for p in (checkpoint_dir / "offsets").glob("*") if p.name.isdigit()]
    if not batches:
        return None
    latest = max(batches, key=lambda p: int(p.name))
    return parse_offset_file(latest.read_text(encoding="utf-8"))
