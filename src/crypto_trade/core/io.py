"""File IO helpers.

Raw writes stay append-only where possible. Raw data is the source of truth
for downstream replay and reprocessing, so on-disk shapes must remain stable.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable, Iterator


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def append_jsonl(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, sort_keys=True, default=str) + "\n")


def iter_jsonl(path: Path) -> Iterator[Any]:
    """Yield decoded JSON objects from a JSONL file.

    Tolerates blank lines and malformed lines: a single corrupted entry never
    aborts replay of the rest of the file.
    """
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, obj: Any) -> None:
    """Atomic JSON write via temp file + replace."""
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True, default=str)
    tmp.replace(path)


def append_csv(path: Path, row: dict[str, Any], fieldnames: list[str]) -> None:
    ensure_dir(path.parent)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def read_csv_col(path: Path, col: str) -> set[str]:
    """Return the set of non-empty values from one column of a CSV file."""
    out: set[str] = set()
    if not path.exists():
        return out
    try:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                val = row.get(col)
                if val:
                    out.add(val)
    except Exception:
        pass
    return out


def chunked(items: list[str], n: int) -> Iterable[list[str]]:
    """Yield successive ``n``-sized chunks from ``items``."""
    for i in range(0, len(items), n):
        yield items[i : i + n]
