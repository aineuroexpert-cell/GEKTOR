# 🎯 GEKTOR v12.0 (APEX) — Macro-Radar Architecture

# 🏗️ SYSTEM STATUS
- **Core Version:** 12.0 (HARDENED)
- **Status:** Operational / Advisory Mode
- **System Target:** Institutional-Grade Signal Discovery (Market-Mirror)
- **Execution Mode:** AMPUTATED (Self-Aware Monitoring)

---

## 🛠️ KEY COMPONENTS

### 🔹 TradeSweeper (L3 Aggregation)
- Reconstructs fragmented trade streams into unified **Institutional Sweeps**.
- Identifies **Aggressor Signatures** (Icebergs, Impulse, Distribution).
- Includes **Pessimistic Sweep Fill (v12.0)** for risk-realistic advisory targets.

### 🔹 Dynamic Universe Shaker
- Background autonomous loop for **monitoring pool rebalancing** (15m interval).
- **Filters:** Turnover > $50M, Spread < 15bps (Liquidity-First Universe).
- Real-time **Shard Rebalancing** on the `NerveCenter` bus.

### 🔹 Macro-Radar Engine
- **Alpha-Neutral Discovery:** Beta-neutralizing CVD against BTC for isolated signals.
- **Stealth CUSUM Detector:** Identifies TWAP/VWAP accumulation via cumulative sum drift.
- **Spatial Basis Audit:** Validates Lead-Lag signals (Perp vs Spot divergence).

### 🔹 Aegis (Decay Sentinel)
- Persistent monitoring of **Signal Expectancy (WR & R-Multiple)** in Redis.
- **Auto-Halt Protocol:** Instantly disables advisory signals on mathematical Alpha decay.
- **Self-Excitation Filter:** Suppresses "feedback loops" (Shadow Liquidity Overdrive).

---

## 🔒 SECURITY & RESILIENCE
- **Zero-GIL Math:** All heavy statistical analysis (Z-Score, MAD, Variance Ratio) runs in `ProcessPoolExecutor`.
- **NerveCenter (Redis-Bus):** Ultra-low latency asynchronous synchronization across distributed shards.
- **StateReconciler:** Automatic REST-based gap filling on WebSocket disconnections.
- **Aegis-Halt:** Manual or automatic global kill-switch for market-breaking events.

---

## 🚀 GETTING STARTED

### 1. Requirements
- Python 3.11+
- PostgreSQL (TimescaleDB) **or** SQLite (default for local dev — fully supported)
- Redis 7.0+ (optional; used by `ReliableIngestionBuffer`, falls back to disk spillover)
- Bybit V5 API access (public WS is enough for the radar; no trade API required)

### 2. Deployment
```bash
# Install dependencies
make install

# Configure
cp .env.example .env       # then edit BOT_TOKEN, CHAT_ID, etc.

# Start APEX Radar (Advisory Mode)
make run-local
```

### 3. Monitoring
- Alerts are dispatched via **TelegramRadarNotifier** through the
  transactional outbox (`outbox_events` table). The outbox SQL works
  on both SQLite and PostgreSQL.
- A **PartialBlindnessWatchdog** raises a Telegram alert if no ticks
  arrive for `WATCHDOG_SILENCE_SEC` (default 60s).
- Status logs at INFO every 60s: `ticks bars signals alerts symbols`.

### 4. Tests
```bash
make test           # full suite (expected: 68 passed, 8 skipped)
make test-radar     # regression suite only (fast)
make test-vpin      # VPIN invariants + Hypothesis property tests
make test-pipeline  # ingestor -> radar end-to-end
```

### 5. Hardened in v3.6.0 APEX-RADAR
See [SINGLE_SOURCE_OF_TRUTH.md](./SINGLE_SOURCE_OF_TRUTH.md) for the
canonical architecture manifesto. Test sentinels guard invariants
I1–I5, time-decay consistency, IEEE-754 drift, polarity contract.
AI agents must read [AGENTS.md](./AGENTS.md) before modifying any
file in this repository.

---

_Created: 2026-04-04 | Updated: 2025-05-21 v3.6.0 APEX-RADAR | Status: HARDENED_
