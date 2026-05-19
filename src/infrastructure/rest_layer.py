from loguru import logger
from typing import Optional

async def fetch_usable_equity(bybit_client) -> float:
    """
    [UNIFIED ONLY] Institutional Equity Fetch. (v21.64.4)
    Гарантирует работу только с UTA-аккаунтами.
    """
    try:
        # BybitRestClient.get_wallet_balance returns float in current impl, 
        # but the user wants to handle the raw response if possible or use a specific UTA path.
        # However, for consistency with the requested code:
        resp = await bybit_client.get_wallet_balance_raw(accountType="UNIFIED")
        
        if not resp or not resp.get("result", {}).get("list"):
            logger.error("❌ [REST] UNIFIED account not found or not upgraded.")
            raise ValueError("UNIFIED account not found or not upgraded")
        
        acc_data = resp["result"]["list"][0]
        # Check for USDT specifically in Unified Account
        coin = next((c for c in acc_data.get("coin", []) if c["coin"] == "USDT"), None)
        
        balance = float(coin["walletBalance"]) if coin else 0.0
        logger.info(f"💰 [REST] Usable Equity (UNIFIED): ${balance:,.2f}")
        return balance
        
    except Exception as e:
        logger.error(f"💥 [REST] Equity fetch failed: {e}")
        raise
