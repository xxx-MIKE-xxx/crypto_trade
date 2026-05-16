"""PumpPortal websocket loop: ingest events, dedupe, and launch token workers."""

from __future__ import annotations

import asyncio
import json

import websockets

from crypto_trade.core.text import compact_json_dumps
from crypto_trade.core.time import utc_now_iso_ms_z
from crypto_trade.ingest.bronze import EventSink, event_timestamp
from crypto_trade.pipeline.config import PipelineConfig
from crypto_trade.pipeline.mint import as_mapping, classify_pumpportal_event, extract_mint
from crypto_trade.pipeline.orchestrator import migrated_token_worker
from crypto_trade.pipeline.state import StateStore


async def pumpportal_loop(
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    stop: asyncio.Event,
) -> None:
    semaphore = asyncio.Semaphore(cfg.max_concurrent_tokens)
    active_tasks: set[asyncio.Task[None]] = set()
    backoff = 1.0

    while not stop.is_set():
        try:
            print(f"[pumpportal] connecting url={cfg.pumpportal_ws_url}", flush=True)
            async with websockets.connect(
                cfg.pumpportal_url, ping_interval=20, ping_timeout=20, close_timeout=5
            ) as ws:
                backoff = 1.0
                await ws.send(compact_json_dumps({"method": "subscribeNewToken"}))
                await ws.send(compact_json_dumps({"method": "subscribeMigration"}))
                await sink.write(
                    source="pipeline",
                    event_type="pumpportal_subscribed",
                    payload={
                        "url": cfg.pumpportal_ws_url,
                        "subscriptions": ["subscribeNewToken", "subscribeMigration"],
                    },
                )
                await sink.flush()
                message_count = 0
                last_message_ts: str | None = None
                print(
                    "[pumpportal] connected; subscribed to new tokens and migrations; waiting for events",
                    flush=True,
                )

                while not stop.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=cfg.heartbeat_seconds)
                    except asyncio.TimeoutError:
                        await sink.flush()
                        active = sum(1 for task in active_tasks if not task.done())
                        print(
                            f"[pumpportal] heartbeat connected=true messages={message_count} "
                            f"active_workers={active} last_message={last_message_ts or 'none'}",
                            flush=True,
                        )
                        continue

                    raw_text = (
                        raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
                    )
                    try:
                        payload = json.loads(raw_text)
                    except json.JSONDecodeError:
                        await sink.write(
                            source="pumpportal",
                            event_type="non_json_message",
                            raw_text=raw_text,
                            payload={"raw": raw_text},
                            level="warning",
                        )
                        print(f"[pumpportal] non-json message bytes={len(raw_text)}", flush=True)
                        continue

                    payload_map = as_mapping(payload)
                    event_type = classify_pumpportal_event(payload_map)
                    mint = extract_mint(payload_map)
                    ev_ts = event_timestamp(payload_map)
                    message_count += 1
                    last_message_ts = utc_now_iso_ms_z()

                    await sink.write(
                        source="pumpportal",
                        event_type=event_type,
                        token_mint=mint,
                        payload=payload,
                        raw_text=raw_text,
                        event_ts=ev_ts,
                    )
                    state.record_seen_event(
                        source="pumpportal",
                        event_type=event_type,
                        mint=mint,
                        event_ts=ev_ts,
                        payload=payload,
                    )

                    if message_count <= 5 or event_type == "migration" or message_count % 100 == 0:
                        print(
                            f"[pumpportal] event #{message_count} type={event_type} "
                            f"mint={mint or 'n/a'}",
                            flush=True,
                        )

                    if mint and event_type == "new_token":
                        state.upsert_new_token(mint, cfg.data_root / "raw" / "tokens" / mint)

                    if mint and event_type == "migration":
                        token_dir = cfg.data_root / "raw" / "tokens" / mint
                        first = state.mark_migrated(mint, token_dir, payload_map)
                        if first:
                            print(
                                f"[pumpportal] migration detected mint={mint}; launching token worker",
                                flush=True,
                            )
                            task = asyncio.create_task(
                                migrated_token_worker(
                                    cfg=cfg,
                                    state=state,
                                    sink=sink,
                                    mint=mint,
                                    migration_event=payload_map,
                                    semaphore=semaphore,
                                )
                            )
                            active_tasks.add(task)
                            task.add_done_callback(active_tasks.discard)
                        else:
                            print(
                                f"[pumpportal] duplicate migration ignored mint={mint}", flush=True
                            )

        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(
                f"[pumpportal] connection error error={exc!r}; reconnecting in {backoff:.1f}s",
                flush=True,
            )
            await sink.write(
                source="pipeline",
                event_type="pumpportal_connection_error",
                payload={"error": repr(exc), "backoff_seconds": backoff},
                level="error",
            )
            await sink.flush()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)

    if active_tasks:
        await asyncio.gather(*active_tasks, return_exceptions=True)
