# crypto_trade

The quant trading system is designed to trade post-migration meme coins. It's comprised of 4 models:
1. M1 - chooses which coins to trade - predicts their survival in next 6-24h
2. M2 - discovers trading opportunities in coins from M1
3. M3 - quantifes quality of the opportunity discovered by M2
4. M4 - ranks all opportunites based on M2 and M3 predictions and chooses position sizing


Model description:
1. M1 - this model is trained on:
    1. Market data: X min of post migration trading, Aggregated metrics from pre-migration trading
    2. Analytics: Scam report
    3. Alternative data: Coins webstie quality, Telegram group metrics, DexScreener SEO metrics

    The target of the model is binary predictions [0,1] with confidence score - where 1 means coin will reach a certain market cap, liquidity and volume in the next 6-24h

    The model architecture is: ...


2. M2 - 

Data acquisition framework:

Do not use ordinary K-fold CV.

Use walk-forward OOS prediction.

For each historical period:
1. Train M1 only on data available before that period.
2. Score all migrated coins in the next period.
3. Record which coins M1 would have accepted.
4. Pull high-resolution data for accepted coins, false positives, near-threshold rejects, and a randomized rejected control sample.
5. Train M2 on the OOS-accepted universe, with oversampling or weighting for false positives.


Data acquired for model M1:
1. Market data: 1h of trading post migration, 20 min of trading pre-migration
2. Analytics: Scam Report
3. Alternative data: Website quality score, telegram group user count, dex screener SEO

Data collected from PumpPortal:
mint
created_at
migration_at



time_to_migration


Data acquisition pipeline for future analysis - with way bigger budget - >100$ / day
    Data is collected from PumpPortal for every coin that passes intial requirements on a rolling window basis

    Coin requirements for starting tracking:

    all pre-migration trades or rolling aggregates

    Features constructed from the PumpPortal data:
    last_20m_buy_count
    last_20m_sell_count
    last_20m_volume_sol
    last_5m_volume_sol
    last_1m_volume_sol
    unique_buyers_20m
    unique_sellers_20m
    buy_sell_ratio
    net_sol_flow
    volume_acceleration
    time_to_migration
    final_5m_share_of_20m_volume


## Package layout

The runtime code lives under `src/crypto_trade/` and follows a layered architecture: shared infrastructure in `core/`, source-specific data acquisition in `ingest/`, enrichment in `features/`, and the live acquisition pipeline in `pipeline/`. Scripts under `scripts/` are thin shims that delegate to the packages.

| Module | Responsibility |
| --- | --- |
| `core/time.py` | UTC timestamp helpers (`utc_now`, `now_ts`, `now_ms`, `now_iso`, `utc_now_iso_z`, `utc_now_iso_ms_z`, `ts_iso`, `parse_event_ts`). |
| `core/io.py` | File I/O: `ensure_dir`, atomic `save_json`, append-only `append_jsonl` / `append_csv`, tolerant `iter_jsonl`, `read_csv_col`, `chunked`. |
| `core/rpc.py` | JSON-RPC primitives: `RpcResult`, `RpcEndpoint`, `RpcPool`, `short_rpc_name`. |
| `core/http.py` | Shared `requests.Session` factory and `get_json` wrapper with rate-limit header capture. |
| `core/env.py` | `load_env` / `get_env` wrappers around `python-dotenv`. |
| `core/text.py` | `compact_json_dumps`, `safe_part` (filesystem partitions), `short_hash`. |
| `core/logging.py` | `configure_logging()` used by every CLI entry point. |
| `ingest/onchain.py` | `Capture` class and `main()` for Solana on-chain capture. |
| `ingest/solana_rpc.py` | Thin Solana JSON-RPC wrappers (`get_transaction`, `get_signatures_for_address`, `get_multiple_accounts`). |
| `ingest/solana_tx.py` | Pure transaction parsing helpers (no I/O). |
| `ingest/bronze.py` | `EventSink` partitioned bronze writer (Parquet with JSONL fallback). |
| `ingest/dexscreener.py` | DexScreener REST probe. |
| `ingest/pumpportal.py` | PumpPortal WebSocket sampler. |
| `features/risk/` | Token risk scoring package (`http_client`, `scoring`, `report`, `cli`). `features/token_risk.py` re-exports the package for backwards compatibility. |
| `features/alt_data/` | Website grader package (`fetch`, `grading`, `cli`). |
| `pipeline/` | Live acquisition orchestrator: `config`, `state` (SQLite), `mint`, `orchestrator`, `pumpportal_loop`, `__main__`. |
| `scripts/` | Thin `runpy` shims that delegate to package modules. |

## Entry points

The package exposes the following console scripts (declared in `pyproject.toml`):

| Command | Target |
| --- | --- |
| `capture-coin` | `crypto_trade.ingest.onchain:main` |
| `run-stream` | `crypto_trade.ingest.pumpportal:main` |
| `probe-dexscreener` | `crypto_trade.ingest.dexscreener:main` |
| `score-token-risk` | `crypto_trade.features.risk.cli:main` |
| `grade-website` | `crypto_trade.features.alt_data.cli:main` |
| `run-pipeline` | `crypto_trade.pipeline.__main__:main` |

## Tests

```bash
pytest tests -q
ruff check src tests
```

The `tests/core/` suite pins the public surface of each `core/*` module (timestamp formats, atomic I/O, tolerant JSONL, HTTP classification, RPC throttling). `tests/ingest/` covers `EventSink` partition layout and the JSONL fallback when `pyarrow` is unavailable.
