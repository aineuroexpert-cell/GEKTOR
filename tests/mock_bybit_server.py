#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║  GEKTOR CHAOS ENGINEERING — Mock Bybit V5 WebSocket Server         ║
║  Version: 1.0 "FAULT INJECTOR"                                     ║
║                                                                      ║
║  Purpose: Deterministic stress-testing of GEKTOR HFT Patches.       ║
║  Emulates: Bybit V5 Linear L2 Orderbook (orderbook.50.BTCUSDT)     ║
║                                                                      ║
║  Fault Commands (type in console):                                   ║
║    normal    — Resume normal orderbook delta stream                  ║
║    drop_data — Stop data, keep answering pings (Data Silence)        ║
║    drop_pong — Keep sending data, ignore pings (Half-Open TCP)       ║
║    skip_seq  — Skip one Sequence ID in next delta (Causal Gap)       ║
║    snapshot  — Send a fresh L2 snapshot (re-sync test)               ║
║    quit      — Shutdown server                                       ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import time
import random
import sys
import threading

try:
    import websockets
    from websockets.server import serve
except ImportError:
    print("❌ Missing dependency. Install: pip install websockets")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

HOST = "0.0.0.0"
PORT = 8765
SYMBOL = "BTCUSDT"
TICK_RATE_HZ = 10        # Deltas per second
BASE_BID = 107_500.0     # Starting best bid
BASE_ASK = 107_500.5     # Starting best ask
DEPTH = 25               # Number of levels per side


# ═══════════════════════════════════════════════════════════════
# GLOBAL FAULT STATE (Thread-safe via GIL for simple booleans)
# ═══════════════════════════════════════════════════════════════

class FaultState:
    """Mutable state container for chaos injection."""
    __slots__ = ('data_silenced', 'pong_silenced', 'skip_next_seq', 'running')

    def __init__(self):
        self.data_silenced = False    # drop_data: stop L2 deltas
        self.pong_silenced = False    # drop_pong: ignore ping frames
        self.skip_next_seq = False    # skip_seq: gap in sequence ID
        self.running = True

FAULT = FaultState()


# ═══════════════════════════════════════════════════════════════
# ORDERBOOK GENERATOR (Deterministic L2)
# ═══════════════════════════════════════════════════════════════

class MockOrderbook:
    """Generates realistic L2 orderbook snapshots and deltas."""

    def __init__(self):
        self.seq_id = 1000
        self.bids = {}  # price_str -> qty_str
        self.asks = {}
        self._init_book()

    def _init_book(self):
        """Seed initial book with 25 levels each side."""
        for i in range(DEPTH):
            bid_price = BASE_BID - (i * 0.5)
            ask_price = BASE_ASK + (i * 0.5)
            bid_qty = round(random.uniform(0.1, 5.0), 3)
            ask_qty = round(random.uniform(0.1, 5.0), 3)
            self.bids[f"{bid_price:.1f}"] = f"{bid_qty:.3f}"
            self.asks[f"{ask_price:.1f}"] = f"{ask_qty:.3f}"

    def generate_snapshot(self) -> dict:
        """Full L2 snapshot (sent on subscribe)."""
        self.seq_id += 1
        return {
            "topic": f"orderbook.50.{SYMBOL}",
            "type": "snapshot",
            "ts": int(time.time() * 1000),
            "data": {
                "s": SYMBOL,
                "b": [[p, q] for p, q in sorted(
                    self.bids.items(), key=lambda x: float(x[0]), reverse=True
                )],
                "a": [[p, q] for p, q in sorted(
                    self.asks.items(), key=lambda x: float(x[0])
                )],
                "u": self.seq_id,
                "seq": self.seq_id
            }
        }

    def generate_delta(self) -> dict:
        """
        Incremental L2 delta with correct Bybit V5 sequence IDs.
        u = last update ID in this message
        U = first update ID in this message
        For single-delta messages: U == u
        """
        # Advance sequence
        prev_u = self.seq_id

        # [CHAOS] Skip a sequence ID if fault is active
        if FAULT.skip_next_seq:
            self.seq_id += 2  # Gap: prev_u=1005, new_u=1007 (1006 missing)
            FAULT.skip_next_seq = False
            print(f"  💥 [INJECTED] Sequence gap: {prev_u} → {self.seq_id} (skipped {prev_u + 1})")
        else:
            self.seq_id += 1

        # Generate 1-3 random level changes per side
        delta_bids = []
        delta_asks = []

        # Mutate 1-2 bid levels
        for _ in range(random.randint(1, 2)):
            level_idx = random.randint(0, DEPTH - 1)
            price = f"{BASE_BID - (level_idx * 0.5):.1f}"
            if random.random() < 0.15:
                # Delete level (qty = 0)
                new_qty = "0"
                self.bids.pop(price, None)
            else:
                new_qty = f"{random.uniform(0.01, 8.0):.3f}"
                self.bids[price] = new_qty
            delta_bids.append([price, new_qty])

        # Mutate 1-2 ask levels
        for _ in range(random.randint(1, 2)):
            level_idx = random.randint(0, DEPTH - 1)
            price = f"{BASE_ASK + (level_idx * 0.5):.1f}"
            if random.random() < 0.15:
                new_qty = "0"
                self.asks.pop(price, None)
            else:
                new_qty = f"{random.uniform(0.01, 8.0):.3f}"
                self.asks[price] = new_qty
            delta_asks.append([price, new_qty])

        return {
            "topic": f"orderbook.50.{SYMBOL}",
            "type": "delta",
            "ts": int(time.time() * 1000),
            "data": {
                "s": SYMBOL,
                "b": delta_bids,
                "a": delta_asks,
                "u": self.seq_id,
                "U": prev_u + 1,  # First update ID (Bybit protocol)
                "seq": self.seq_id
            }
        }

    def generate_trade(self) -> dict:
        """Occasional publicTrade message for realism."""
        mid = (BASE_BID + BASE_ASK) / 2
        price = mid + random.uniform(-2.0, 2.0)
        volume = round(random.uniform(0.001, 0.5), 4)
        side = random.choice(["Buy", "Sell"])
        return {
            "topic": f"publicTrade.{SYMBOL}",
            "type": "snapshot",
            "ts": int(time.time() * 1000),
            "data": [{
                "T": int(time.time() * 1000),
                "s": SYMBOL,
                "S": side,
                "v": str(volume),
                "p": f"{price:.1f}",
                "i": str(random.randint(100000, 999999)),
                "BT": "false"
            }]
        }


# ═══════════════════════════════════════════════════════════════
# WEBSOCKET HANDLER
# ═══════════════════════════════════════════════════════════════

async def handle_client(websocket):
    """Handle a single GEKTOR client connection."""
    client_addr = websocket.remote_address
    print(f"🟢 [CONNECTED] Client: {client_addr}")

    book = MockOrderbook()
    subscribed = False

    # Task: Listen for subscribe messages from client
    async def recv_loop():
        nonlocal subscribed
        try:
            async for message in websocket:
                data = json.loads(message)
                op = data.get("op", "")

                if op == "subscribe":
                    subscribed = True
                    # Send subscription confirmation
                    await websocket.send(json.dumps({
                        "success": True,
                        "ret_msg": "",
                        "op": "subscribe",
                        "conn_id": "mock-gektor-test"
                    }))
                    # Send initial snapshot
                    snapshot = book.generate_snapshot()
                    await websocket.send(json.dumps(snapshot))
                    print(f"  📦 Snapshot sent (seq={book.seq_id}, {DEPTH} levels/side)")

                elif op == "ping":
                    # [CHAOS] drop_pong: ignore ping
                    if FAULT.pong_silenced:
                        print(f"  🔇 [FAULT] Ping received but PONG SUPPRESSED")
                        continue
                    # Normal pong response (Bybit format)
                    await websocket.send(json.dumps({
                        "op": "pong",
                        "args": [str(int(time.time() * 1000))],
                        "ts": int(time.time() * 1000),
                        "ret_msg": "pong"
                    }))

        except websockets.exceptions.ConnectionClosed:
            pass

    # Task: Send L2 deltas at TICK_RATE_HZ
    async def send_loop():
        tick_interval = 1.0 / TICK_RATE_HZ
        trade_counter = 0

        while FAULT.running:
            if not subscribed:
                await asyncio.sleep(0.1)
                continue

            # [CHAOS] drop_data: suppress all market data
            if FAULT.data_silenced:
                await asyncio.sleep(0.5)
                continue

            try:
                # Send orderbook delta
                delta = book.generate_delta()
                await websocket.send(json.dumps(delta))

                # Every 5th tick, send a trade too
                trade_counter += 1
                if trade_counter % 5 == 0:
                    trade = book.generate_trade()
                    await websocket.send(json.dumps(trade))

            except websockets.exceptions.ConnectionClosed:
                break

            await asyncio.sleep(tick_interval)

    # Run both loops concurrently
    try:
        await asyncio.gather(recv_loop(), send_loop())
    except Exception as e:
        print(f"🔴 [DISCONNECTED] Client {client_addr}: {e}")
    finally:
        print(f"🔴 [DISCONNECTED] Client {client_addr}")


# ═══════════════════════════════════════════════════════════════
# CONSOLE COMMAND LISTENER (Stdin in separate thread)
# ═══════════════════════════════════════════════════════════════

def stdin_listener():
    """
    Blocking stdin reader in a daemon thread.
    Mutates FAULT state based on operator commands.
    """
    HELP = """
╔══════════════════════════════════════════════════════════════╗
║  CHAOS COMMANDS:                                             ║
║    normal    — Resume normal operation                       ║
║    drop_data — Stop sending data (test 15s watchdog)         ║
║    drop_pong — Ignore pings (test 30s half-open detection)   ║
║    skip_seq  — Skip next sequence ID (test SeqGuard)         ║
║    snapshot  — (info) Client must re-subscribe for snapshot   ║
║    status    — Show current fault state                       ║
║    quit      — Shutdown server                                ║
╚══════════════════════════════════════════════════════════════╝
"""
    print(HELP)

    while FAULT.running:
        try:
            cmd = input("chaos> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            FAULT.running = False
            break

        if cmd == "normal":
            FAULT.data_silenced = False
            FAULT.pong_silenced = False
            FAULT.skip_next_seq = False
            print("✅ [NORMAL] All faults cleared. Nominal operation.")

        elif cmd == "drop_data":
            FAULT.data_silenced = True
            FAULT.pong_silenced = False
            print("🔇 [FAULT ACTIVE] Data SILENCED. Pongs still active.")
            print("   → GEKTOR should kill socket in ~15 seconds.")

        elif cmd == "drop_pong":
            FAULT.pong_silenced = True
            FAULT.data_silenced = False
            print("🔇 [FAULT ACTIVE] Pongs SUPPRESSED. Data still flowing.")
            print("   → GEKTOR should detect half-open in ~30 seconds.")

        elif cmd == "skip_seq":
            FAULT.skip_next_seq = True
            print("💥 [FAULT ARMED] Next delta will SKIP a sequence ID.")
            print("   → GEKTOR SeqGuard should fire CRITICAL + request re-sync.")

        elif cmd == "status":
            print(f"   data_silenced = {FAULT.data_silenced}")
            print(f"   pong_silenced = {FAULT.pong_silenced}")
            print(f"   skip_next_seq = {FAULT.skip_next_seq}")

        elif cmd == "quit":
            FAULT.running = False
            print("🛑 [SERVER] Shutting down...")
            break

        elif cmd == "help":
            print(HELP)

        elif cmd == "":
            continue

        else:
            print(f"❓ Unknown command: '{cmd}'. Type 'help' for options.")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

async def main():
    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  🎯 GEKTOR CHAOS ENGINE — Mock Bybit V5 Server                 ║
║  Listening on: ws://{HOST}:{PORT}                               ║
║  Symbol: {SYMBOL}  |  Tick Rate: {TICK_RATE_HZ} Hz              ║
║                                                                  ║
║  GEKTOR connect URL: ws://localhost:{PORT}/v5/public/linear      ║
╚══════════════════════════════════════════════════════════════════╝
""")

    # Start stdin listener in background thread
    console_thread = threading.Thread(target=stdin_listener, daemon=True)
    console_thread.start()

    # Start WebSocket server
    # process_request allows any path (GEKTOR connects to /v5/public/linear)
    async with serve(
        handle_client,
        HOST,
        PORT,
        ping_interval=None,   # We handle pings manually (Bybit protocol, not WS standard)
        ping_timeout=None,
        close_timeout=5,
    ):
        # Keep server alive until quit command
        while FAULT.running:
            await asyncio.sleep(0.5)

    print("⬛ [SERVER] Terminated.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Server stopped by operator.")
