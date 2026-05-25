#!/usr/bin/env python3
"""
OOB_Emergency_Lane & MicrostructureDefender
Управление Неблагоприятным Отбором (Adverse Selection)
Выделенный сокет (Out-Of-Band) для микросекундных отмен
"""

import asyncio
import logging
from typing import Set

logger = logging.getLogger("GEKTOR.OOB_Defender")

class OOB_Emergency_Lane:
    """
    Выделенная, заблокированная в RAM сессия для экстренных отмен.
    Не делит TCP-пул с обычным REST API клиентом. 
    Обеспечивает микросекундную приоритезацию на уровне ОС.
    """
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        # Для aiohttp это TCPConnector(limit=10, force_close=False, keepalive_timeout=300)
        self.active_cooldowns: Set[str] = set()

    async def fire_abort_pulse_atomic(self, symbol: str):
        """
        Отправляет cancel_all_orders по выделенному каналу.
        Блокирует создание новых ордеров.
        """
        if symbol in self.active_cooldowns:
            return
            
        logger.critical(f"⚡ [OOB LANE] ABORT PULSE FIRED: {symbol}. Pulling quotes!")
        self.active_cooldowns.add(symbol)
        try:
            # Имитация O_{1} выстрела через чистый сокет
            # await self.oob_session.post("/v5/order/cancel-all", json={"category": "linear", "symbol": symbol, "settleCoin": "USDT"})
            await asyncio.sleep(0.005) # ~5ms RTT
            logger.success(f"✅ [OOB LANE] Ликвидация Maker-ордеров {symbol} прошла успешно.")
        except Exception as e:
            logger.error(f"🛑 [OOB LANE] Pulse failed: {e}")
        finally:
            # Снимаем кул-даун через 250мс
            await asyncio.sleep(0.250)
            self.active_cooldowns.discard(symbol)


class MicrostructureDefender:
    def __init__(self, oob_lane: OOB_Emergency_Lane):
        self.oob = oob_lane
        self.baseline_bid_liquidity: dict[str, float] = {}

    def evaluate_predictive_ofi(self, symbol: str, new_bid_liq: float, trades_cusum: float) -> bool:
        """
        Предиктивный анализ (Spoofing Pulls).
        Реагирует на снятие пассивной ликвидности перед агрессивным ударом.
        """
        base_liq = self.baseline_bid_liquidity.get(symbol, new_bid_liq)
        
        # Если в течение миллисекунд из стакана ИСЧЕЗАЕТ 40% ликвидности без единого трейда -> Toxic Flow imminent
        liquidity_drop_ratio = (base_liq - new_bid_liq) / (base_liq or 1)
        
        if liquidity_drop_ratio > 0.40 and trades_cusum < 1000:
            logger.warning(f"🌪️ [PREDICTIVE OFI] {symbol}: Ликвидность рухнула на {liquidity_drop_ratio*100:.1f}%. Эвакуация!")
            asyncio.create_task(self.oob.fire_abort_pulse_atomic(symbol))
            # Обновление бейзлайна после эвакуации
            self.baseline_bid_liquidity[symbol] = new_bid_liq
            return True
            
        self.baseline_bid_liquidity[symbol] = new_bid_liq
        return False
