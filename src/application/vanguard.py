import asyncio
from typing import Set, List, Dict, Optional
import time
from loguru import logger

class VanguardScanner:
    """
    [GEKTOR v2.0] Dynamic Asset Universe Manager.
    Solves the "Complacency vs Scaling" dilemma by focusing MathCore resources 
    only on instruments with confirmed institutional volume.
    """
    def __init__(self, rest_client, orchestrator, max_symbols: int = 8):
        self.client = rest_client
        self.orchestrator = orchestrator
        self.max_symbols = max_symbols
        self.active_universe: Set[str] = set(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        self._lock = asyncio.Lock()
        self._running = False

    async def start(self):
        """
        Background task to rebalance the trading universe.
        [Lifecycle] Runs every 15 minutes.
        """
        logger.info("🔭 [Vanguard] Scanner ENGAGED. Monitoring for hot liquidity flow.")
        self._running = True
        while self._running:
            try:
                # 1. Fetch 24h tickers to find "Top Movers" by turnover
                tickers = await self.client.get_tickers() # Assuming standard wrapper
                
                # 2. Score and filter candidates (Turnover > 50M USD)
                hot_candidates = self._filter_top_liquidity(tickers)
                
                # 3. Rebalance with ZERO-DOWNTIME
                await self._rebalance(hot_candidates)
                
            except Exception as e:
                logger.error(f"⚠️ [Vanguard] Periodic scan failed: {e}")
            
            await asyncio.sleep(900)

    async def scan_market_cycle(self):
        """Backward-compatible alias for legacy callers."""
        await self.start()

    def _filter_top_liquidity(self, tickers: List[Dict]) -> Set[str]:
        """Filters assets with sufficient liquidity for VPIN stability."""
        candidates = []
        for t in tickers:
            symbol = t['symbol']
            if not symbol.endswith('USDT'): continue
            
            turnover = float(t.get('turnover24h', 0))
            # [STRENGTH FILTER] Minimum 50M USD turnover to prevent manipulation noise
            if turnover < 50_000_000: continue 
            
            candidates.append((symbol, turnover))
            
        candidates.sort(key=lambda x: x[1], reverse=True)
        # Keep space for BTC/ETH/SOL and select top survivors
        top_hot = [c[0] for c in candidates[:self.max_symbols - 3]]
        return set(top_hot)

    async def _rebalance(self, hot_candidates: Set[str]):
        """
        [COLD START PROTECTION] Orchestrates REST hydration for new assets.
        Ensures MathCore doesn't start blind.
        """
        async with self._lock:
            target_universe = hot_candidates.union({"BTCUSDT", "ETHUSDT", "SOLUSDT"})
            
            to_add = target_universe - self.active_universe
            to_remove = self.active_universe - target_universe
            
            for symbol in to_remove:
                logger.info(f"🧊 [Vanguard] Cooling down: {symbol}. Releasing resources.")
                await self.orchestrator.stop_monitoring(symbol)
                self.active_universe.remove(symbol)

            for symbol in to_add:
                logger.warning(f"🔥 [Vanguard] Detected institutional interest: {symbol}. HYDRATING...")
                
                # [REST HYDRATION] Solving the Cold Start 50-bar penalty
                # We fetch recent trades via REST to seed the VPIN buffer instantly.
                recent_trades = await self.client.get_recent_trades(symbol, limit=1000)
                
                if await self.orchestrator.start_monitoring(symbol, seed_data=recent_trades):
                    self.active_universe.add(symbol)
                    logger.success(f"🚀 [Vanguard] {symbol} ARMED and PRE-WARMED.")
                else:
                    logger.error(f"❌ [Vanguard] Failed to hydrate {symbol}. Skipping.")

    def stop(self):
        self._running = False
