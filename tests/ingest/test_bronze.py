"""Tests for :mod:`crypto_trade.ingest.bronze`.

The sink is the bronze write path for every upstream source, so we pin both the
partition layout and the row shape. Parquet support is exercised when pyarrow
is installed; the JSONL fallback is exercised via monkeypatching when it isn't.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from crypto_trade.ingest import bronze


def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


def test_event_timestamp_prefers_payload_field():
    out = bronze.event_timestamp({"timestamp": "2023-11-14T22:13:20Z"})
    assert out == "2023-11-14T22:13:20.000Z"


def test_event_timestamp_falls_back_to_now():
    out = bronze.event_timestamp({})
    assert out.endswith("Z")


def test_sink_writes_rows_via_jsonl_fallback(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(bronze, "pa", None, raising=False)
    monkeypatch.setattr(bronze, "pq", None, raising=False)

    async def go():
        sink = bronze.EventSink(tmp_path, batch_size=10)
        sink.schema = None
        for i in range(3):
            await sink.write(
                source="unit_test",
                event_type="probe",
                payload={"i": i, "timestamp": "2023-11-14T22:13:20Z"},
                token_mint="MintA",
            )
        await sink.flush()

    asyncio.run(go())

    out_dir = tmp_path / "bronze" / "source=unit_test" / "date=2023-11-14" / "token_mint=MintA"
    files = list(out_dir.glob("part-*.jsonl"))
    assert len(files) == 1
    rows = [json.loads(line) for line in files[0].read_text(encoding="utf-8").splitlines()]
    assert [r["event_json"] for r in rows] == [
        '{"i":0,"timestamp":"2023-11-14T22:13:20Z"}',
        '{"i":1,"timestamp":"2023-11-14T22:13:20Z"}',
        '{"i":2,"timestamp":"2023-11-14T22:13:20Z"}',
    ]
    assert all(r["source"] == "unit_test" for r in rows)
    assert all(r["token_mint"] == "MintA" for r in rows)


def test_sink_partitions_global_when_mint_missing(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(bronze, "pa", None, raising=False)
    monkeypatch.setattr(bronze, "pq", None, raising=False)

    async def go():
        sink = bronze.EventSink(tmp_path, batch_size=10)
        sink.schema = None
        await sink.write(
            source="src",
            event_type="evt",
            payload={"timestamp": "2024-01-02T03:04:05Z"},
        )
        await sink.flush()

    asyncio.run(go())

    assert (tmp_path / "bronze" / "source=src" / "date=2024-01-02" / "token_mint=_global").exists()


@pytest.mark.skipif(bronze.pa is None, reason="pyarrow not installed")
def test_sink_writes_parquet_when_pyarrow_available(tmp_path: Path):
    import pyarrow.parquet as pq

    async def go():
        sink = bronze.EventSink(tmp_path, batch_size=10)
        await sink.write(
            source="parq",
            event_type="probe",
            payload={"timestamp": "2024-05-06T07:08:09Z", "v": 1},
            token_mint="MintB",
        )
        await sink.flush()

    asyncio.run(go())

    files = list(
        (tmp_path / "bronze" / "source=parq" / "date=2024-05-06" / "token_mint=MintB").glob("*.parquet")
    )
    assert len(files) == 1
    parquet_file = pq.ParquetFile(files[0])
    assert parquet_file.metadata.num_rows == 1
    column_names = set(parquet_file.schema_arrow.names)
    assert {"event_ts", "source", "token_mint", "event_json"} <= column_names
