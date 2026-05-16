"""Data acquisition pipeline package.

Splits the former ``scripts/run_data_acquisition_pipeline.py`` monolith into
focused modules:

* :mod:`crypto_trade.pipeline.config` -- :class:`PipelineConfig` from CLI/env.
* :mod:`crypto_trade.pipeline.state` -- SQLite job/token store.
* :mod:`crypto_trade.pipeline.mint` -- mint extraction and event classification.
* :mod:`crypto_trade.pipeline.orchestrator` -- subprocess + per-token worker.
* :mod:`crypto_trade.pipeline.pumpportal_loop` -- PumpPortal websocket loop.
* :mod:`crypto_trade.pipeline.__main__` -- CLI entrypoint.
"""
