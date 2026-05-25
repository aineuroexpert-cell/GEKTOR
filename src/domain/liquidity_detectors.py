"""
[GEKTOR APEX v3.6.2] Liquidity event detectors (no-warmup, instant-fire).

These three detectors complement the VPIN ring buffer engine. They look
at raw trade ticks (not bars), so they fire immediately as soon as the
event happens — no 50-bar warmup required. They are essential for an
active trader who watches a wide universe of low-cap altcoins, where
the dollar-bar warmup would otherwise take hours per symbol.

  Sweep         — N+ same-side aggressor trades sum to > $threshold
                  within W seconds. Catches the moment large players
                  walk the book.
  Large Print   — a single trade > k% of the symbol's 24h turnover.
                  Catches block prints / OTC clearing on-screen.
  OFI Pulse     — 1-minute order-flow imbalance > C * rolling median.
                  Catches sudden directional pressure breakouts.

All three are O(1) per tick and pre-allocated (no GC churn). They share
the same `LiquidityAlert` dataclass and the same downstream wiring
(rate limiter → outbox → Telegram).

CRITICAL INVARIANTS:
  L1.  Per-symbol isolation: a sweep on BTC must not affect SOL state.
       Each detector keeps a `dict[symbol, _state]` and never crosses.
  L2.  Polarity contract identical to VPIN engine: `is_buyer_maker=True`
       means the TAKER sold to a resting buyer. Sweep / OFI Pulse use
       the TAKER side as direction ("buy" = taker bought, aggressor
       lifted ask; "sell" = taker sold, aggressor hit bid).
  L3.  Time monotonicity: detectors use the exchange timestamp `ts`,
       not `time.monotonic()`, because backfill / replay scenarios
       can rewind monotonic time.
  L4.  Threshold validation: all numerical thresholds must be > 0;
       fail-fast at __init__.

Forbidden in this file (mirrors vpin_engine.py):
  - `import` inside hot path methods.
  - Allocating containers per-tick.
  - Silent `except Exception: pass`.
"""
from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

# ----------------------------------------------------------------------
# Public alert type
# ----------------------------------------------------------------------


@dataclass(slots=True)
class LiquidityAlert:
    symbol: str
    timestamp: float
    kind: Literal["SWEEP", "LARGE_PRINT", "OFI_PULSE"]
    direction: Literal["buy", "sell"]
    price: float
    notional_usd: float
    # Free-form metadata so each detector can attach human-readable
    # context (number of trades, percent-of-turnover, etc) without
    # bloating the dataclass.
    extra: dict[str, float | int | str] = field(default_factory=dict)


# ----------------------------------------------------------------------
# Sweep
# ----------------------------------------------------------------------


@dataclass(slots=True)
class _SweepState:
    direction: Literal["buy", "sell"] | None
    notional_usd: float
    count: int
    first_ts: float
    last_ts: float


class SweepDetector:
    """Fires when N+ consecutive same-side aggressor trades sum to >
    `min_notional_usd` within `window_sec` seconds. Per-symbol state.

    Edge cases handled:
      * Direction flips reset the accumulator (a buy → sell sequence
        is two separate sweeps, not one mixed sweep).
      * Window slide is "since first_ts": if the trail is older than
        the window, the accumulator resets on the next tick.
      * After firing, the accumulator resets (one alert per sweep).
    """

    __slots__ = (
        "_min_trades",
        "_window_sec",
        "_min_notional_usd",
        "_state",
        "_cooldown_sec",
        "_last_alert_ts",
    )

    def __init__(
        self,
        min_trades: int = 5,
        window_sec: float = 30.0,
        min_notional_usd: float = 100_000.0,
        cooldown_sec: float = 60.0,
    ) -> None:
        if min_trades < 2:
            raise ValueError("min_trades must be >= 2")
        if window_sec <= 0:
            raise ValueError("window_sec must be > 0")
        if min_notional_usd <= 0:
            raise ValueError("min_notional_usd must be > 0")
        if cooldown_sec < 0:
            raise ValueError("cooldown_sec must be >= 0")

        self._min_trades = min_trades
        self._window_sec = window_sec
        self._min_notional_usd = min_notional_usd
        self._cooldown_sec = cooldown_sec
        self._state: dict[str, _SweepState] = {}
        self._last_alert_ts: dict[str, float] = {}

    def process_tick(
        self,
        symbol: str,
        is_buyer_maker: bool,
        price: float,
        size: float,
        ts: float,
    ) -> LiquidityAlert | None:
        """Ingest one tick. Returns a LiquidityAlert when a sweep fires.

        Polarity (invariant L2):
            is_buyer_maker = True  → taker sold (aggressor=sell)
            is_buyer_maker = False → taker bought (aggressor=buy)
        """
        # Cooldown: if we just fired, suppress further sweep alerts for
        # this symbol for `cooldown_sec`. Z-score-like sustained sweeps
        # would otherwise flood the channel.
        last_alert = self._last_alert_ts.get(symbol)
        if last_alert is not None and (ts - last_alert) < self._cooldown_sec:
            # Still update state so subsequent sweeps see fresh data.
            self._reset(symbol)
            return None

        direction: Literal["buy", "sell"] = "sell" if is_buyer_maker else "buy"
        notional = price * size

        st = self._state.get(symbol)

        # Conditions to start a fresh accumulator:
        #   (a) no state yet,
        #   (b) direction flipped,
        #   (c) window expired.
        if (
            st is None
            or st.direction != direction
            or (ts - st.first_ts) > self._window_sec
        ):
            self._state[symbol] = _SweepState(
                direction=direction,
                notional_usd=notional,
                count=1,
                first_ts=ts,
                last_ts=ts,
            )
            return None

        # Accumulate.
        st.notional_usd += notional
        st.count += 1
        st.last_ts = ts

        if (
            st.count >= self._min_trades
            and st.notional_usd >= self._min_notional_usd
        ):
            alert = LiquidityAlert(
                symbol=symbol,
                timestamp=ts,
                kind="SWEEP",
                direction=direction,
                price=price,
                notional_usd=st.notional_usd,
                extra={
                    "trade_count": st.count,
                    "duration_sec": ts - st.first_ts,
                    "min_trades": self._min_trades,
                    "min_notional_usd": self._min_notional_usd,
                },
            )
            self._last_alert_ts[symbol] = ts
            self._reset(symbol)
            return alert

        return None

    def _reset(self, symbol: str) -> None:
        self._state.pop(symbol, None)


# ----------------------------------------------------------------------
# Large Print
# ----------------------------------------------------------------------


TurnoverProvider = Callable[[str], float]
"""Callable returning 24h turnover (USD) for a symbol, or 0.0 if unknown."""


class LargePrintDetector:
    """Fires when a single trade's notional > `pct_threshold` × 24h turnover.

    `pct_threshold = 0.005` (0.5%) means: a single print bigger than half
    a percent of the symbol's daily turnover is suspicious.
    """

    __slots__ = (
        "_turnover_provider",
        "_pct_threshold",
        "_min_notional_usd",
        "_cooldown_sec",
        "_last_alert_ts",
    )

    def __init__(
        self,
        turnover_provider: TurnoverProvider,
        pct_threshold: float = 0.005,
        min_notional_usd: float = 25_000.0,
        cooldown_sec: float = 60.0,
    ) -> None:
        if pct_threshold <= 0:
            raise ValueError("pct_threshold must be > 0")
        if min_notional_usd <= 0:
            raise ValueError("min_notional_usd must be > 0")
        if cooldown_sec < 0:
            raise ValueError("cooldown_sec must be >= 0")
        self._turnover_provider = turnover_provider
        self._pct_threshold = pct_threshold
        self._min_notional_usd = min_notional_usd
        self._cooldown_sec = cooldown_sec
        self._last_alert_ts: dict[str, float] = {}

    def process_tick(
        self,
        symbol: str,
        is_buyer_maker: bool,
        price: float,
        size: float,
        ts: float,
    ) -> LiquidityAlert | None:
        notional = price * size

        # Absolute floor: filters dust from low-liquidity contracts whose
        # 0.5% threshold would otherwise be like $5 (instant spam).
        if notional < self._min_notional_usd:
            return None

        # Cooldown.
        last_alert = self._last_alert_ts.get(symbol)
        if last_alert is not None and (ts - last_alert) < self._cooldown_sec:
            return None

        turnover_24h = self._turnover_provider(symbol)
        if turnover_24h <= 0.0:
            # Unknown turnover → fall back to absolute threshold only.
            # Still fire if notional >= 10× the min floor.
            if notional < 10.0 * self._min_notional_usd:
                return None
            pct = float("nan")
        else:
            pct = notional / turnover_24h
            if pct < self._pct_threshold:
                return None

        direction: Literal["buy", "sell"] = "sell" if is_buyer_maker else "buy"
        alert = LiquidityAlert(
            symbol=symbol,
            timestamp=ts,
            kind="LARGE_PRINT",
            direction=direction,
            price=price,
            notional_usd=notional,
            extra={
                "pct_of_24h_turnover": pct,
                "turnover_24h_usd": turnover_24h,
                "pct_threshold": self._pct_threshold,
            },
        )
        self._last_alert_ts[symbol] = ts
        return alert


# ----------------------------------------------------------------------
# OFI Pulse
# ----------------------------------------------------------------------


@dataclass(slots=True)
class _OFIBucket:
    bucket_start_ts: float
    buy_usd: float
    sell_usd: float


class OFIPulseDetector:
    """Bucket-based order-flow imbalance pulse detector.

    Maintains per-symbol rolling deques of 1-minute (or configured)
    OFI buckets. When the most recent bucket's |OFI| exceeds `k` times
    the median |OFI| over the trailing window, fires.

    Warmup: ~history_buckets / 2 (median is more stable after a few).
    A single bucket isn't enough — we want at least 5 prior buckets
    before firing, so the median is meaningful.
    """

    __slots__ = (
        "_bucket_sec",
        "_history_buckets",
        "_k",
        "_min_notional_usd",
        "_min_history",
        "_cooldown_sec",
        "_buckets",
        "_last_alert_ts",
    )

    def __init__(
        self,
        bucket_sec: float = 60.0,
        history_buckets: int = 60,
        k: float = 3.0,
        min_notional_usd: float = 50_000.0,
        min_history: int = 5,
        cooldown_sec: float = 120.0,
    ) -> None:
        if bucket_sec <= 0:
            raise ValueError("bucket_sec must be > 0")
        if history_buckets < 2:
            raise ValueError("history_buckets must be >= 2")
        if k <= 1.0:
            raise ValueError("k must be > 1.0")
        if min_history < 2:
            raise ValueError("min_history must be >= 2")

        self._bucket_sec = bucket_sec
        self._history_buckets = history_buckets
        self._k = k
        self._min_notional_usd = min_notional_usd
        self._min_history = min_history
        self._cooldown_sec = cooldown_sec
        self._buckets: dict[str, deque[_OFIBucket]] = {}
        self._last_alert_ts: dict[str, float] = {}

    def process_tick(  # noqa: PLR0911 — early-return ladder is clearer than nested ifs
        self,
        symbol: str,
        is_buyer_maker: bool,
        price: float,
        size: float,
        ts: float,
    ) -> LiquidityAlert | None:
        notional = price * size

        bq = self._buckets.get(symbol)
        if bq is None:
            bq = deque(maxlen=self._history_buckets)
            self._buckets[symbol] = bq

        # Snap ts to the bucket boundary.
        bucket_start = ts - (ts % self._bucket_sec)

        # Reuse the head bucket if it's the current one; else open a new.
        if bq and bq[-1].bucket_start_ts == bucket_start:
            cur = bq[-1]
        else:
            cur = _OFIBucket(bucket_start_ts=bucket_start, buy_usd=0.0, sell_usd=0.0)
            bq.append(cur)

        if is_buyer_maker:
            cur.sell_usd += notional
        else:
            cur.buy_usd += notional

        # Need enough history for a meaningful median.
        if len(bq) < self._min_history + 1:
            return None

        # Compute current bucket's signed OFI and absolute magnitude.
        cur_ofi = cur.buy_usd - cur.sell_usd
        cur_abs = abs(cur_ofi)
        if cur_abs < self._min_notional_usd:
            return None

        # Median absolute OFI over the prior buckets (exclude current).
        prior_abs = [
            abs(b.buy_usd - b.sell_usd) for b in bq if b is not cur
        ]
        if not prior_abs:
            return None
        prior_abs.sort()
        n = len(prior_abs)
        if n % 2 == 1:
            median_abs = prior_abs[n // 2]
        else:
            median_abs = 0.5 * (prior_abs[n // 2 - 1] + prior_abs[n // 2])

        if median_abs <= 0.0:
            # Median is zero (no historical OFI). Require a clearly
            # non-trivial absolute pulse to avoid noise.
            if cur_abs < 5.0 * self._min_notional_usd:
                return None
            ratio = float("inf")
        else:
            ratio = cur_abs / median_abs
            if ratio < self._k:
                return None

        # Cooldown.
        last_alert = self._last_alert_ts.get(symbol)
        if last_alert is not None and (ts - last_alert) < self._cooldown_sec:
            return None

        direction: Literal["buy", "sell"] = "buy" if cur_ofi > 0 else "sell"
        alert = LiquidityAlert(
            symbol=symbol,
            timestamp=ts,
            kind="OFI_PULSE",
            direction=direction,
            price=price,
            notional_usd=cur_abs,
            extra={
                "ratio_to_median": ratio,
                "k_threshold": self._k,
                "median_abs_ofi_usd": median_abs,
                "bucket_sec": self._bucket_sec,
                "history_buckets_used": n,
            },
        )
        self._last_alert_ts[symbol] = ts
        return alert


# ----------------------------------------------------------------------
# Composite bank
# ----------------------------------------------------------------------


class LiquidityDetectorBank:
    """Aggregates the three liquidity detectors and runs them on every tick.

    Each detector independently produces zero or one alert per tick.
    The bank collects all non-None alerts and returns them as a list.
    Downstream code (RadarPipeline) decides what to do with them.
    """

    __slots__ = ("sweep", "large_print", "ofi_pulse", "_enabled")

    def __init__(
        self,
        sweep: SweepDetector | None,
        large_print: LargePrintDetector | None,
        ofi_pulse: OFIPulseDetector | None,
    ) -> None:
        self.sweep = sweep
        self.large_print = large_print
        self.ofi_pulse = ofi_pulse
        self._enabled = any(d is not None for d in (sweep, large_print, ofi_pulse))

    @property
    def enabled(self) -> bool:
        return self._enabled

    def process_tick(
        self,
        symbol: str,
        is_buyer_maker: bool,
        price: float,
        size: float,
        ts: float,
    ) -> list[LiquidityAlert]:
        if not self._enabled:
            return []
        out: list[LiquidityAlert] = []
        if self.sweep is not None:
            a = self.sweep.process_tick(symbol, is_buyer_maker, price, size, ts)
            if a is not None:
                out.append(a)
        if self.large_print is not None:
            a = self.large_print.process_tick(symbol, is_buyer_maker, price, size, ts)
            if a is not None:
                out.append(a)
        if self.ofi_pulse is not None:
            a = self.ofi_pulse.process_tick(symbol, is_buyer_maker, price, size, ts)
            if a is not None:
                out.append(a)
        return out

