"""
This file tests currently the helius api / websocket stream for one / multiple coins. ]
It uses getTransactionForAdress, accountSubscribe, getPriorityFeeEstimate, getRecentPerformanceSamples, simulateTransaction
The file works standalone and saves output to tmp/onchain/<mint>/transactions.json, account_state.jsonl, priority_fee.jsonl, performance_samples.jsonl, simulted_transactions.jsonl (optional)

python -m crypto_trade.ingest.test 
--mint FWdgp1fdWkDc5FWeZcJZwGjHtBiBHg1fySWmKURJpump
--capture-time 360
--rpc-interval 5      

"""



from crypto_trade.ingest.dexscreener import stream_trading_info_multi_coin, stream_trading_info_one_coin
import argparse
import json
from pathlib import Path
from crypto_trade.core.paths import TMP_DIR
from crypto_trade.core.io import append_jsonl
import asyncio
from crypto_trade.core.rpc import RPC
from crypto_trade.core.time import now_ms


OUTPUT_DIR = TMP_DIR / "onchain"


def save_paths():
     return [OUTPUT_DIR / "transactions.json", OUTPUT_DIR / "account_state.jsonl",  OUTPUT_DIR / "priority_fee.jsonl", OUTPUT_DIR / "performance_samples.jsonl",  OUTPUT_DIR / "simulted_transactions.jsonl"]

async def poll_rpc(rpc, mint, interval, length, pool_address, token_vault_address, sol_vault_address):
    length_ms = length * 1000
    start_time = now_ms()
    transactions_path, account_state_path, priority_fee_path, perf_samples_path, sim_trans_path = save_paths()
    method =  "getPriorityFeeEstimate"
    params = [
                {
                "accountKeys": [pool_address, token_vault_address, sol_vault_address],
                "options": {
                    "priorityLevel": "High"
                }
                }
            ]
    while True:
        resp1 = await rpc.call_rpc(method[0], params[0])
        append_jsonl(priority_fee_path, json.dumps(resp1))
        asyncio.sleep(interval1)
        
    while True:
        resp2 = await rpc.call_rpc(method[1], params[1])
        append_jsonl(perf_samples_path, json.dumps(resp2))
        asyncio.sleep(interval2)
    
    while True:
        resp3 = await rpc.call_rpc(method[2], params[2])
        append_jsonl(transaction_path, json.dumps(resp3))
        asyncio.sleep(interval3)

    while True:
        resp4 = await rpc.call_rpc(method[3], params[3])
        append_jsonl(sim_trans_path, json.dumps(resp4))
        asyncio.sleep(interval4)

    if now_ms() - start_time >= length_ms:
            return 


async def stream_websocket(rpc, mint, length,  pool_address, token_vault_address, sol_vault_address):
    start_time = now_ms()
    account_state_path = save_paths()[1]
    method = ""
    params = [
         {"pubkey": ...,
          "encoding": ...
         }
    ]

    rpc.connect_websocket(params, method, account_state_path)
    
    





async def main(mint, capture_time, rpc_interval):
    rpc = RPC()
    await asyncio.gather(  
         poll_rpc(),
         stream_websocket() 
    )

    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mint", help="solana token mint address")
    parser.add_argument("--capture_time", default=1800, help="length of data collection window in seconds")
    parser.add_argument("--rpc_interval", default=60, help="interval between requests for priority fees and performance stats - 60 recommended")
    args = parser.parse_args()
    asyncio.run(main(args.mint, args.caputre_time, args.rpc_interval))