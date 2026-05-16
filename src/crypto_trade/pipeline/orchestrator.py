"""Per-token worker: capture subprocess, DexScreener polling, and report stages."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shlex
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    import httpx
except ImportError as exc:  # pragma: no cover
    raise SystemExit("Missing dependency: pip install httpx") from exc

from crypto_trade.ingest.bronze import EventSink
from crypto_trade.pipeline.config import PipelineConfig
from crypto_trade.pipeline.mint import as_mapping
from crypto_trade.pipeline.state import StateStore


def context_for_token(cfg: PipelineConfig, mint: str) -> dict[str, str]:
    token_dir = cfg.data_root / "raw" / "tokens" / mint
    onchain_dir = token_dir / "onchain"
    reports_dir = token_dir / "reports"
    logs_dir = token_dir / "logs"
    for d in (onchain_dir, reports_dir, logs_dir):
        d.mkdir(parents=True, exist_ok=True)

    return {
        "python": sys.executable,
        "repo_root": str(cfg.repo_root),
        "data_root": str(cfg.data_root),
        "token_dir": str(token_dir),
        "onchain_dir": str(onchain_dir),
        "reports_dir": str(reports_dir),
        "logs_dir": str(logs_dir),
        "mint": mint,
        "token_mint": mint,
        "token_address": mint,
        "duration_sec": str(cfg.capture_seconds),
        "chain": cfg.chain_id,
        "chain_id": cfg.chain_id,
    }


def split_command(command: str) -> list[str]:
    # Use shlex with posix mode off on Windows to keep quoting behavior intact.
    return shlex.split(command, posix=(os.name != "nt"))


def default_script_cmd(
    cfg: PipelineConfig, script_name: str, mint: str, extra: Sequence[str] = ()
) -> list[str]:
    script_path = cfg.repo_root / "scripts" / script_name
    return [sys.executable, str(script_path), "--mint", mint, *extra]


def command_from_template(
    env_name: str, default_cmd: Sequence[str], context: Mapping[str, str]
) -> list[str]:
    template = os.getenv(env_name)
    if not template:
        return list(default_cmd)
    return split_command(template.format_map(defaultdict(str, context)))


def env_for_child(cfg: PipelineConfig, context: Mapping[str, str]) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "PYTHONUNBUFFERED": "1",
            "PIPELINE_ACTIVE": "1",
            "PIPELINE_REPO_ROOT": str(cfg.repo_root),
            "PIPELINE_DATA_ROOT": str(cfg.data_root),
            "TOKEN_MINT": context["mint"],
            "MINT": context["mint"],
            "TOKEN_ADDRESS": context["mint"],
            "CHAIN_ID": cfg.chain_id,
            "CAPTURE_SECONDS": str(cfg.capture_seconds),
            "ONCHAIN_OUT_DIR": context["onchain_dir"],
            "REPORTS_OUT_DIR": context["reports_dir"],
        }
    )
    return env


async def run_subprocess_stage(
    *,
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    mint: str,
    stage: str,
    command: Sequence[str],
    context: Mapping[str, str],
    timeout_seconds: float | None = None,
) -> int:
    logs_dir = Path(context["logs_dir"])
    logs_dir.mkdir(parents=True, exist_ok=True)
    stdout_log = logs_dir / f"{stage}.stdout.log"
    stderr_log = logs_dir / f"{stage}.stderr.log"

    if cfg.dry_run:
        print(f"[dry-run] {stage} {mint}: {' '.join(command)}", flush=True)
        await sink.write(
            source="pipeline",
            event_type="dry_run_command",
            token_mint=mint,
            payload={"stage": stage, "command": list(command)},
        )
        return 0

    job_id = state.start_job(mint, stage, command)
    print(f"[{stage}] starting mint={mint} cmd={' '.join(command)}", flush=True)
    env = env_for_child(cfg, context)
    proc = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(cfg.repo_root),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    async def drain(
        stream: asyncio.StreamReader | None, path: Path, stream_name: str, level: str
    ) -> None:
        if stream is None:
            return
        with path.open("ab") as fh:
            while True:
                line = await stream.readline()
                if not line:
                    break
                fh.write(line)
                fh.flush()
                text = line.decode("utf-8", errors="replace").rstrip("\r\n")
                event_type = f"{stage}_{stream_name}"
                with contextlib.suppress(json.JSONDecodeError):
                    payload = json.loads(text)
                    await sink.write(
                        source=stage,
                        event_type=event_type,
                        token_mint=mint,
                        payload=payload,
                        raw_text=text,
                        stream=stream_name,
                        level=level,
                    )
                    continue
                await sink.write(
                    source=stage,
                    event_type=event_type,
                    token_mint=mint,
                    payload={"line": text},
                    raw_text=text,
                    stream=stream_name,
                    level=level,
                )

    stdout_task = asyncio.create_task(drain(proc.stdout, stdout_log, "stdout", "info"))
    stderr_task = asyncio.create_task(drain(proc.stderr, stderr_log, "stderr", "error"))

    try:
        rc = await asyncio.wait_for(proc.wait(), timeout=timeout_seconds)
        await asyncio.gather(stdout_task, stderr_task)
        state.finish_job(
            job_id,
            status="ok" if rc == 0 else "failed",
            return_code=rc,
            stdout_log=stdout_log,
            stderr_log=stderr_log,
        )
        print(f"[{stage}] finished mint={mint} rc={rc}", flush=True)
        return rc
    except asyncio.TimeoutError:
        proc.terminate()
        with contextlib.suppress(ProcessLookupError):
            await asyncio.wait_for(proc.wait(), timeout=15)
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        state.finish_job(
            job_id,
            status="timeout",
            return_code=proc.returncode,
            stdout_log=stdout_log,
            stderr_log=stderr_log,
            error=f"timeout after {timeout_seconds} seconds",
        )
        print(f"[{stage}] timeout mint={mint} after {timeout_seconds} seconds", flush=True)
        return 124
    except Exception as exc:
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
        state.finish_job(
            job_id,
            status="error",
            return_code=proc.returncode,
            stdout_log=stdout_log,
            stderr_log=stderr_log,
            error=repr(exc),
        )
        raise


async def run_with_arg_fallback(
    *,
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    mint: str,
    stage: str,
    command: Sequence[str],
    fallback_command: Sequence[str],
    context: Mapping[str, str],
    timeout_seconds: float | None = None,
) -> int:
    rc = await run_subprocess_stage(
        cfg=cfg,
        state=state,
        sink=sink,
        mint=mint,
        stage=stage,
        command=command,
        context=context,
        timeout_seconds=timeout_seconds,
    )
    if rc == 2 and list(command) != list(fallback_command):
        # argparse often exits 2 on unknown flags. Rerun with env-only mode.
        await sink.write(
            source="pipeline",
            event_type="argparse_fallback",
            token_mint=mint,
            payload={"stage": stage, "from": list(command), "to": list(fallback_command)},
        )
        rc = await run_subprocess_stage(
            cfg=cfg,
            state=state,
            sink=sink,
            mint=mint,
            stage=f"{stage}_fallback",
            command=fallback_command,
            context=context,
            timeout_seconds=timeout_seconds,
        )
    return rc


async def poll_dexscreener_until_visible(
    *,
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    mint: str,
    context: Mapping[str, str],
) -> Any | None:
    url = f"https://api.dexscreener.com/token-pairs/v1/{cfg.chain_id}/{mint}"
    deadline = time.monotonic() + cfg.dex_timeout_seconds
    print(
        f"[dexscreener] polling mint={mint} every {cfg.dex_poll_seconds}s "
        f"for up to {cfg.dex_timeout_seconds}s",
        flush=True,
    )

    async with httpx.AsyncClient(timeout=15.0, headers={"Accept": "application/json"}) as client:
        while time.monotonic() < deadline:
            try:
                resp = await client.get(url)
                payload = resp.json() if resp.content else None
                await sink.write(
                    source="dexscreener_poll",
                    event_type="token_pairs_snapshot",
                    token_mint=mint,
                    payload={"status_code": resp.status_code, "url": url, "body": payload},
                )
                if resp.status_code == 200:
                    pairs = payload if isinstance(payload, list) else as_mapping(payload).get("pairs")
                    if isinstance(pairs, list) and len(pairs) > 0:
                        state.mark_dex_visible(mint, payload)
                        print(f"[dexscreener] visible mint={mint} pairs={len(pairs)}", flush=True)
                        return payload
            except Exception as exc:
                await sink.write(
                    source="dexscreener_poll",
                    event_type="poll_error",
                    token_mint=mint,
                    payload={"url": url, "error": repr(exc)},
                    level="error",
                )

            await asyncio.sleep(cfg.dex_poll_seconds)

    print(f"[dexscreener] visibility timeout mint={mint}", flush=True)
    await sink.write(
        source="dexscreener_poll",
        event_type="visibility_timeout",
        token_mint=mint,
        payload={"url": url, "timeout_seconds": cfg.dex_timeout_seconds},
        level="warning",
    )
    return None


async def run_reports_after_dex_visible(
    *,
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    mint: str,
    context: Mapping[str, str],
) -> None:
    scripts = {
        "website_grader": (
            "PIPELINE_WEBSITE_CMD_TEMPLATE",
            default_script_cmd(cfg, "grade_website.py", mint, ["--out-dir", context["reports_dir"]]),
        ),
        "risk_report": (
            "PIPELINE_RISK_REPORT_CMD_TEMPLATE",
            default_script_cmd(cfg, "build_risk_report.py", mint, ["--out-dir", context["reports_dir"]]),
        ),
        "token_risk_score": (
            "PIPELINE_TOKEN_RISK_CMD_TEMPLATE",
            default_script_cmd(cfg, "score_token_risk.py", mint, ["--out-dir", context["reports_dir"]]),
        ),
        "dexscreener_report": (
            "PIPELINE_DEX_REPORT_CMD_TEMPLATE",
            default_script_cmd(
                cfg, "probe_dexscreener.py", mint, ["--chain", cfg.chain_id, "--out-dir", context["reports_dir"]]
            ),
        ),
    }

    telegram_template = os.getenv("PIPELINE_TELEGRAM_CMD_TEMPLATE")
    telegram_script = cfg.repo_root / "scripts" / "telegram_group_report.py"
    if telegram_template or telegram_script.exists():
        default_telegram = [
            sys.executable,
            str(telegram_script),
            "--mint",
            mint,
            "--out-dir",
            context["reports_dir"],
        ]
        scripts["telegram_group_report"] = ("PIPELINE_TELEGRAM_CMD_TEMPLATE", default_telegram)
    else:
        await sink.write(
            source="pipeline",
            event_type="telegram_report_skipped",
            token_mint=mint,
            payload={
                "reason": "No scripts/telegram_group_report.py and PIPELINE_TELEGRAM_CMD_TEMPLATE is unset. "
                "Set PIPELINE_TELEGRAM_CMD_TEMPLATE if Telegram reporting lives under another entrypoint."
            },
            level="warning",
        )

    for stage, (env_name, default_cmd) in scripts.items():
        cmd = command_from_template(env_name, default_cmd, context)
        fallback = [sys.executable, str(cfg.repo_root / "scripts" / Path(default_cmd[1]).name)]
        rc = await run_with_arg_fallback(
            cfg=cfg,
            state=state,
            sink=sink,
            mint=mint,
            stage=stage,
            command=cmd,
            fallback_command=fallback,
            context=context,
            timeout_seconds=cfg.command_timeout_seconds,
        )
        if rc != 0:
            await sink.write(
                source="pipeline",
                event_type="report_stage_failed",
                token_mint=mint,
                payload={"stage": stage, "return_code": rc},
                level="error",
            )


async def migrated_token_worker(
    *,
    cfg: PipelineConfig,
    state: StateStore,
    sink: EventSink,
    mint: str,
    migration_event: Mapping[str, Any],
    semaphore: asyncio.Semaphore,
) -> None:
    async with semaphore:
        print(f"[token-worker] started mint={mint}", flush=True)
        context = context_for_token(cfg, mint)
        token_dir = Path(context["token_dir"])
        state.mark_migrated(mint, token_dir, migration_event)
        state.mark_status(mint, "capturing")

        capture_default = default_script_cmd(
            cfg,
            "capture_coin_1h.py",
            mint,
            ["--duration-sec", str(cfg.capture_seconds), "--out-dir", context["onchain_dir"]],
        )
        capture_cmd = command_from_template(
            "PIPELINE_CAPTURE_CMD_TEMPLATE", capture_default, context
        )
        capture_fallback = [sys.executable, str(cfg.repo_root / "scripts" / "capture_coin_1h.py")]

        async def capture_task() -> int:
            return await run_with_arg_fallback(
                cfg=cfg,
                state=state,
                sink=sink,
                mint=mint,
                stage="helius_capture_1h",
                command=capture_cmd,
                fallback_command=capture_fallback,
                context=context,
                timeout_seconds=cfg.capture_seconds + 120,
            )

        async def dex_and_reports_task() -> None:
            visible = await poll_dexscreener_until_visible(
                cfg=cfg, state=state, sink=sink, mint=mint, context=context
            )
            if visible is not None:
                await run_reports_after_dex_visible(
                    cfg=cfg, state=state, sink=sink, mint=mint, context=context
                )

        results = await asyncio.gather(
            capture_task(), dex_and_reports_task(), return_exceptions=True
        )
        failures = [
            r for r in results if isinstance(r, Exception) or (isinstance(r, int) and r != 0)
        ]
        final_status = "failed" if failures else "reports_done"
        state.mark_status(mint, final_status)
        print(f"[token-worker] finished mint={mint} status={final_status}", flush=True)
