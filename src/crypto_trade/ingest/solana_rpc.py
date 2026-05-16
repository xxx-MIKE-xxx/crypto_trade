"""Solana-specific JSON-RPC method wrappers around :class:`core.rpc.RpcPool`."""

from __future__ import annotations

from crypto_trade.core.rpc import RpcPool, RpcResult

PUBLIC_RPC = "https://api.mainnet-beta.solana.com"
PUBLIC_WS = "wss://api.mainnet-beta.solana.com"
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def get_transaction(pool: RpcPool, signature: str) -> tuple[RpcResult, str]:
    return pool.call_any(
        "getTransaction",
        [
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    )


def get_signatures_for_address(
    pool: RpcPool, address: str, limit: int
) -> tuple[RpcResult, str]:
    return pool.call_any(
        "getSignaturesForAddress",
        [
            address,
            {
                "limit": limit,
                "commitment": "confirmed",
            },
        ],
    )


def get_multiple_accounts(
    pool: RpcPool, addresses: list[str]
) -> tuple[RpcResult, str]:
    return pool.call_any(
        "getMultipleAccounts",
        [
            addresses,
            {
                "encoding": "base64",
                "commitment": "confirmed",
            },
        ],
    )
