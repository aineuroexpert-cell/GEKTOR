#!/usr/bin/env python3
"""
L6StateHealer — Production-grade state reconciliation engine for GEKTOR-STRIKE
Handles OOM-torn spillover.jsonl + REST Oracle (Zero-Trust)
Military HFT standard: Deterministic State, O(1) Memory, Smart Epoch Transition
"""

import asyncio
import logging
import time
import orjson
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, Set, List
from itertools import islice

logger = logging.getLogger("GEKTOR.L6StateHealer")

def chunked_iterable(iterable, size):
    """Генератор для нарезки списка на батчи (куски)"""
    it = iter(iterable)
    for first in it:
        yield [first] + list(islice(it, size - 1))

class StateHealth(Enum):
    CLEAN = auto()
    TAINTED_TORN_WRITE = auto()
    TAINTED_REST_FAILED = auto()
    SAFE_HOLD = auto()

@dataclass
class EpochData:
    exchange_hwm_ms: int = 0
    active_intents: Set[str] = field(default_factory=set)

@dataclass
class ReconciliationResult:
    health: StateHealth
    must_manual_review: bool = False

class LedgerProjection:
    def __init__(self):
        self.state: Dict[str, Any] = {}

    def apply_event_delta(self, event: Dict[str, Any], epoch: EpochData):
        event_ts = int(event.get("E", event.get("ts", 0)))
        
        # Sequence Guard
        if event_ts < epoch.exchange_hwm_ms and epoch.exchange_hwm_ms > 0:
            return

        topic = event.get("topic", "")
        data = event.get("data", [{}])[0]
        symbol = data.get("symbol", event.get("symbol"))
        
        if not symbol:
            return

        if symbol not in self.state:
            self.state[symbol] = {"position_size": 0.0, "entry_price": 0.0, "active_orders": {}}

        if "position" in topic:
            self.state[symbol]["position_size"] = float(data.get("size", 0))
            self.state[symbol]["entry_price"] = float(data.get("avgPrice", 0))
            
        elif "order" in topic:
            order_id = data.get("orderId")
            
            # Epoch Isolation Whitelist
            if order_id not in epoch.active_intents:
                return
                
            status = data.get("orderStatus")
            if status in ("Filled", "Cancelled", "Deactivated"):
                self.state[symbol]["active_orders"].pop(order_id, None)
                epoch.active_intents.discard(order_id)
            else:
                self.state[symbol]["active_orders"][order_id] = data

class L6StateHealer:
    def __init__(self, bybit_client, telegram_notifier=None):
        self.bybit_client = bybit_client
        self.telegram = telegram_notifier
        self.health = StateHealth.CLEAN
        self.ledger = LedgerProjection()
        self.epoch = EpochData()
        self._max_rest_retries = 7

    async def hydrate(self, spillover_path: str = "artifacts/spillover.jsonl") -> ReconciliationResult:
        result = ReconciliationResult(health=StateHealth.CLEAN)
        self.ledger = LedgerProjection()
        self.epoch = EpochData()
        
        try:
            with open(spillover_path, "rb") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        event = orjson.loads(line)
                        self.ledger.apply_event_delta(event, self.epoch)
                    except orjson.JSONDecodeError:
                        logger.critical(f"💀 [DB] TORN WRITE at line {line_num}. STATE IS TAINTED.")
                        result.health = StateHealth.TAINTED_TORN_WRITE
                        result.must_manual_review = True
                        break
        except FileNotFoundError:
            logger.warning("⚠️ [HYDRATION] Spillover пуст — холодный старт.")
            self.health = StateHealth.CLEAN
            return result

        if result.health == StateHealth.TAINTED_TORN_WRITE:
            rest_ok = await self._force_smart_epoch_transition(result)
            if not rest_ok:
                result.health = StateHealth.SAFE_HOLD
                await self._enter_safe_hold()

        self.health = result.health
        return result

    async def _execute_smart_epoch_transition(self):
        logger.warning("🔄 [EPOCH] Старт Smart Transition. Запрос Оракула...")
        
        open_orders_response = await self.bybit_client.get_open_orders(category="linear", settleCoin="USDT")
        exchange_time = int(open_orders_response.get("time", 0))
        
        if not exchange_time:
            raise RuntimeError("CRITICAL: Oracle REST response invalid or empty.")

        orders_list = open_orders_response.get("result", {}).get("list", [])
        
        zombie_ids_to_purge = []
        survivors_count = 0
        
        for order in orders_list:
            order_id = order.get("orderId")
            symbol = order.get("symbol")
            
            # Cross-Diffing against Ledger
            if symbol in self.ledger.state and order_id in self.ledger.state[symbol].get("active_orders", {}):
                survivors_count += 1
                self.epoch.active_intents.add(order_id)
                self.epoch.active_intents.add(order.get("orderLinkId", ""))
                
                # Protect against Partial Fill blind spots
                self.ledger.state[symbol]["active_orders"][order_id]["leavesQty"] = float(order.get("leavesQty", 0))
                self.ledger.state[symbol]["active_orders"][order_id]["cumExecQty"] = float(order.get("cumExecQty", 0))
            else:
                zombie_ids_to_purge.append({"symbol": symbol, "orderId": order_id})

        # Batch-Purge
        if zombie_ids_to_purge:
            logger.warning(f"🧟‍♂️ [PURGE] Найдено {len(zombie_ids_to_purge)} зомби. Батч-аннигиляция...")
            # Chunking list to groups of 10 or less
            for batch in chunked_iterable(zombie_ids_to_purge, 10):
                try:
                    await self.bybit_client.cancel_batch_order(category="linear", request=batch)
                except Exception as e:
                    logger.error(f"🛑 [PURGE] Ошибка батч-отмены: {e}. Зомби могут быть живы.")

        # Rebuild positions via sequence guard
        positions = await self.bybit_client.get_active_positions()
        for p in positions:
            sym = p.get("symbol")
            size = float(p.get("size", 0))
            if sym and size > 0:
                if sym not in self.ledger.state:
                    self.ledger.state[sym] = {"active_orders": {}}
                self.ledger.state[sym]["position_size"] = size
                self.ledger.state[sym]["entry_price"] = float(p.get("avgPrice", 0))

        self.epoch.exchange_hwm_ms = exchange_time
        logger.success(f"✅ [EPOCH] Спасенных Maker-ордеров: {survivors_count}. Ватерлиния: {exchange_time}ms.")

    async def _force_smart_epoch_transition(self, result: ReconciliationResult) -> bool:
        for attempt in range(1, self._max_rest_retries + 1):
            try:
                await self._execute_smart_epoch_transition()
                result.health = StateHealth.CLEAN
                return True
            except Exception as e:
                wait = min(1.5 ** attempt, 15)
                logger.warning(f"⚠️ [ORACLE] Отказ (попытка {attempt}): {e}. Ждем {wait:.1f}s")
                await asyncio.sleep(wait)
        logger.critical("🛑 [ORACLE] REST МЕРТВ. Оракул недоступен.")
        return False

    async def _enter_safe_hold(self):
        logger.critical("🛑 [SAFE_HOLD] Торговля заблокирована. Требуется ручной перезапуск.")
        if hasattr(self.telegram, "notify_manual"):
            await self.telegram.notify_manual("🚨 <b>[GEKTOR] SAFE_HOLD</b>\nREST мертв, стейт грязный. Ручной аудит!")

    def is_trading_allowed(self) -> bool:
        return self.health == StateHealth.CLEAN

    def is_emergency_close_allowed(self) -> bool:
        return True
