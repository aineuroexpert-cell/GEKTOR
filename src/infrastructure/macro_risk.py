#!/usr/bin/env python3
"""
Macro Risk Manager & Distributed Consensus Guard
Защита от Split-Brain и Cross-Exchange Delta-Neutral Hedging с синтетическим локом.
"""

import asyncio
import logging
import aiohttp
from typing import Optional

logger = logging.getLogger("GEKTOR.MacroRisk")

class DistributedConsensusGuard:
    """Защита от Split-Brain. Определяет природу блэкаута."""
    def __init__(self):
        self.control_endpoints = [
            "https://api.binance.com/api/v3/ping",
            "https://1.1.1.1",
            "https://8.8.8.8"
        ]
        self.bybit_health_endpoint = "https://api.bybit.com/v5/public/time"

    async def verify_blackout(self) -> str:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.bybit_health_endpoint, timeout=2.0) as resp:
                    if resp.status == 200:
                        return "FALSE_ALARM"
            except (asyncio.TimeoutError, aiohttp.ClientError):
                pass 

            world_alive = False
            for endpoint in self.control_endpoints:
                try:
                    async with session.get(endpoint, timeout=1.0) as resp:
                        if resp.status in (200, 404, 403):
                            world_alive = True
                            break
                except Exception:
                    continue

            if world_alive:
                logger.critical("🚨 [CONSENSUS] Мир жив, Bybit МЕРТВ. Это глобальный Blackout Bybit.")
                return "BYBIT_DEAD"
            else:
                logger.critical("🔌 [CONSENSUS] Внешний мир недоступен. Лежит НАШ провайдер. ХЕДЖИРОВАТЬ ЗАПРЕЩЕНО.")
                return "LOCAL_NETWORK_DEAD"

class MacroRiskManager:
    def __init__(self, binance_client, bybit_margin_tracker):
        self.binance = binance_client
        self.margin_tracker = bybit_margin_tracker

    async def execute_shadow_hedge(self, symbol: str, net_delta: float):
        liq_price = self.margin_tracker.get_liquidation_price(symbol)
        hedge_qty = abs(net_delta)
        side = "BUY" if net_delta < 0 else "SELL"

        logger.warning(f"🛡️ [HEDGE] Активация Shadow Hedge на Binance: {side} {hedge_qty} {symbol}")
        
        try:
            hedge_order = await self.binance.place_market_order(symbol, side, hedge_qty)
            
            close_side = "SELL" if side == "BUY" else "BUY"
            await self.binance.place_limit_order(
                symbol, 
                side=close_side, 
                qty=hedge_qty, 
                price=liq_price, 
                reduce_only=True
            )
            logger.success(f"✅ [HEDGE] Дельта заморожена. Синтетический лок на {liq_price} установлен.")
        except Exception as e:
            logger.critical(f"🛑 [HEDGE] Провал хеджирования: {e}")
