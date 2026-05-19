import asyncio
import time
from dataclasses import dataclass
from typing import List

@dataclass
class SignalPayload:
    asset: str
    anomaly: str
    kde_distance: float
    regime: str
    action: str

class TelegramGateway:
    """
    Decoupled Async Gateway with Conflation (Batching) and Token Bucket Rate Limiter.
    Operates on a dirty core (Core 0/1) via asyncio.Queue to avoid blocking the Quant Engine.
    """
    def __init__(self, queue: asyncio.Queue, bot_token: str, chat_id: str):
        self.queue = queue
        self.bot_token = bot_token
        self.chat_id = chat_id
        
        # Token Bucket State for Telegram (Max 20 msgs per minute for group chats, but we use a safer 1 msg/sec threshold)
        self.bucket_capacity = 1.0
        self.tokens = 1.0
        self.fill_rate = 1.0  # 1 token per second
        self.last_fill = time.monotonic()
        
        self.conflation_threshold = 3
        self.conflation_window = 0.250  # 250ms dynamic buffering window

    async def _consume_token(self) -> None:
        """Token Bucket Rate Limiter Wait."""
        while True:
            now = time.monotonic()
            elapsed = now - self.last_fill
            self.tokens = min(self.bucket_capacity, self.tokens + elapsed * self.fill_rate)
            self.last_fill = now
            
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            
            await asyncio.sleep(1.0 - self.tokens)

    async def _dispatch_http(self, text: str) -> None:
        """Mock HTTP dispatch."""
        await self._consume_token()
        # aiohttp POST to Telegram API goes here
        # print(f"[TELEGRAM API SEND] \n{text}\n")

    def _format_single(self, signal: SignalPayload) -> str:
        return (
            f"🎯 <b>Advisory Signal</b>\n"
            f"Asset: {signal.asset}\n"
            f"Anomaly: {signal.anomaly}\n"
            f"KDE Distance: {signal.kde_distance:+.2f}%\n"
            f"Regime: {signal.regime}\n"
            f"Action: <b>{signal.action}</b>"
        )

    def _format_batch(self, signals: List[SignalPayload]) -> str:
        # Prioritize by absolute KDE distance
        sorted_signals = sorted(signals, key=lambda s: abs(s.kde_distance), reverse=True)
        top_signals = sorted_signals[:5]
        
        text = f"🔥 <b>MASS ANOMALY DETECTED: {len(signals)} ASSETS</b>\n\n"
        text += f"Top 5 Extreme Deviations:\n"
        for s in top_signals:
            text += f"• {s.asset} | {s.anomaly} | KDE: {s.kde_distance:+.2f}%\n"
        
        text += f"\nAction: <b>ELEVATED VOLATILITY. SYSTEM SWITCHED TO SNIPER MODE.</b>"
        return text

    async def run(self) -> None:
        """Consumer loop running on OS scheduler."""
        while True:
            try:
                # Wait for the first signal in a potential batch
                first_signal: SignalPayload = await self.queue.get()
                batch = [first_signal]
                
                # Dynamic Conflation Window (Time-based aggregation)
                try:
                    # Wait up to 250ms for more signals
                    await asyncio.sleep(self.conflation_window)
                    
                    # Drain everything that arrived during the window
                    while not self.queue.empty():
                        batch.append(self.queue.get_nowait())
                except asyncio.CancelledError:
                    break

                if len(batch) >= self.conflation_threshold:
                    payload_text = self._format_batch(batch)
                else:
                    payload_text = "\n\n".join([self._format_single(s) for s in batch])
                
                asyncio.create_task(self._dispatch_http(payload_text))
                
            except asyncio.CancelledError:
                break
