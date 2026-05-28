import asyncio
import json
from websockets.asyncio.client import connect
import logging
import argparse
import base64
import websockets
from solders.keypair import Keypair
from solders.hash import Hash
from solders.pubkey import Pubkey
from solders.transaction import VersionedTransaction
from solders.system_program import transfer, TransferParams
from solders.message import MessageV0
from crypto_trade.core.logging_config import configure_logging
from crypto_trade.core.time import now_ts
from crypto_trade.core.env import get_env, load_env
from crypto_trade.core.http import request_json
from crypto_trade.core.paths import ENV_FILE
from crypto_trade.core.io import append_jsonl

logger = logging.getLogger(__name__)
load_env()

HELIUS_API_KEY = get_env("HELIUS_API_KEY")
HELIUS_BASE = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
private_key = get_env("SOLANA_WALLET_SECRET_KEY_BYTES")

class RPC:
    def __init__(self, api_key=HELIUS_API_KEY, private_key=private_key, url=HELIUS_BASE):
        self.api_key = api_key
        self.url = url
        self.private_key = private_key
        self.request_id = 0
        self.keypair = self.load_wallet_keypair(self.private_key)

    async def call_rpc(self, method, params: list):
        self.request_id += 1
        body = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params or []
        }
        response = await request_json(
            "POST",
            self.url,
            headers={"Content-Type": "application/json"},
            json=body
        )
        if response.error_type:
            logger.warning(
                "HELIUS RPC request failed %s %s",
                response.error_type,
                response.error_message
            )
        if isinstance(response.data, dict) and response.data.get("error"):
            logger.warning("HELIUS RPC request failed %s", response.data["error"])
        return response
    
    async def simulate_transaction(self, encoded_tx:str):
        method = "simulateTransaction"
        params = [encoded_tx, {"encoding": "base64", "commitment": "confirmed", "sigVerify": False}]
        response = await self.call_rpc(method, params)
        data = response.data.get("result")
        return data
    
    async def make_transaction(self, recipient, amount_lamports:int):
        latest_block_hash = await self.call_rpc(method="getLatestBlockhash", params= [{"commitment": "confirmed"}])
        latest_block_hash=latest_block_hash.data["result"]["value"]["blockhash"]
        latest_block_hash=Hash.from_string(latest_block_hash)
        instruction = transfer(
            TransferParams(
                from_pubkey=self.keypair.pubkey(),
                to_pubkey=Pubkey.from_string(recipient),
                lamports=amount_lamports
            )
        )
        message=MessageV0.try_compile(
            payer=self.keypair.pubkey(),
            instructions=[instruction],
            address_lookup_table_accounts=[],
            recent_blockhash=latest_block_hash
        )
        transaction=VersionedTransaction(message=message, keypairs=[self.keypair])
        encoded_tx=base64.b64encode(bytes(transaction)).decode("utf-8")
        return encoded_tx
    
    async def connect_websocket(self, params, method, file_path, ping_interval=20, ping_timeout=20):
        subscribe_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        url = f"wss://mainnet.helius-rpc.com/?api-key={self.api_key}"
        async with websockets.connect(
            url, 
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
        ) as ws:
            await ws.send(json.dumps(subscribe_msg))

            response = json.loads(await ws.recv())
            logger.info("Subscription response: %s", response)

            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                append_jsonl(file_path, data)

    @staticmethod
    def load_wallet_keypair(keypair_bytes):
        keypair = json.loads(keypair_bytes)
        keypair = Keypair.from_bytes(bytes(keypair))
        return keypair

async def main():
    configure_logging()
    load_env()
    private_key = get_env("SOLANA_WALLET_SECRET_KEY_BYTES")
    rpc = RPC(api_key=HELIUS_API_KEY, private_key=private_key, url=HELIUS_BASE)
    "transaction simulation"
    encoded_tx=await rpc.make_transaction(recipient="6uwUupqP7jzGFGiH4VJXSfwpzuNZrdVahUVzYCqPERW2", amount_lamports=10)
    output=await rpc.simulate_transaction(encoded_tx)
    return output
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    #parser.add_argument("")

    args = parser.parse_args()
    output = asyncio.run(main())
    print(output)
    with open("tmp/rpc.json", "w", encoding="utf-8") as f:
        json.dump(output, indent=2, ensure_ascii=False, fp=f)

