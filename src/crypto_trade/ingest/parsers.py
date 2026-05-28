def classify_transaction(tx, token_vault, sol_vault):
    if tx.get("meta", {}).get("err") is not None:
        return "failed"

    vault_deltas = get_vault_deltas(tx, token_vault, sol_vault)

    if not vault_deltas:
        return "non_simple"

    if is_direct_pool_swap(tx, token_vault, sol_vault):
        if vault_deltas["sol"] > 0 and vault_deltas["token"] < 0:
            return "buy"
        if vault_deltas["sol"] < 0 and vault_deltas["token"] > 0:
            return "sell"

    if uses_router_or_multiple_swaps(tx):
        return "routed"

    return "non_simple"