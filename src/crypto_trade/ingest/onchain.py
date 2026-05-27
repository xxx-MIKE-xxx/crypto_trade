import asnycio

from crypto_trade.core.rpc import RPC


'Subscribe to websocket'
account_hash = ...

params = [
    {
      "accountInclude": [account_hash]
    },
    {
      "commitment": "processed",
      "encoding": "jsonParsed",
      "transactionDetails": "full",
      "showRewards": True,
      "maxSupportedTransactionVersion": 0
    }
  ]


{
  "jsonrpc": "2.0",
  "id": 420,
  "method": "transactionSubscribe",
  "params": [
    {
      "accountInclude": ["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"]
    },
    {
      "commitment": "processed",
      "encoding": "jsonParsed",
      "transactionDetails": "full",
      "showRewards": true,
      "maxSupportedTransactionVersion": 0
    }
  ]
}