import pytest
import time
from src.infrastructure.conflation import DirectionalConflationBuffer

def test_directional_conflation_throughput():
    """
    [STRESS TEST] High-velocity ingestion.
    Simulates 10,000 ticks for a single symbol.
    Verifies that total volume is preserved and aggregated.
    """
    buffer = DirectionalConflationBuffer(critical_size=5000)
    symbol = "BTCUSDT"
    
    # Ingest 10,000 ticks (5000 Buy, 5000 Sell)
    start_time = time.perf_counter()
    for i in range(5000):
        buffer.ingest_immediate({
            "symbol": symbol, "price": 100.0, "volume": 1.0, 
            "side": "Buy", "timestamp": int(time.time() * 1000)
        })
        buffer.ingest_immediate({
            "symbol": symbol, "price": 100.0, "volume": 1.5, 
            "side": "Sell", "timestamp": int(time.time() * 1000)
        })
    end_time = time.perf_counter()
    
    # Throughput check (should be well under 0.1s for 10k ticks in memory)
    duration = end_time - start_time
    assert duration < 0.1, f"Conflation too slow: {duration:.4f}s"
    
    # Flush and verify
    mega_ticks = buffer.flush()
    assert len(mega_ticks) == 2 # One Buy, One Sell
    
    buy_tick = next(t for t in mega_ticks if t["side"] == "Buy")
    sell_tick = next(t for t in mega_ticks if t["side"] == "Sell")
    
    assert buy_tick["volume"] == 5000.0
    assert sell_tick["volume"] == 7500.0 # 5000 * 1.5
    
def test_conflation_latest_price_preservation():
    """
    Ensures that only the LATEST price and timestamp are kept for the mega-tick.
    """
    buffer = DirectionalConflationBuffer()
    symbol = "ETHUSDT"
    
    buffer.ingest_immediate({"symbol": symbol, "price": 2000.0, "volume": 1.0, "side": "Buy", "timestamp": 100})
    buffer.ingest_immediate({"symbol": symbol, "price": 2005.0, "volume": 1.0, "side": "Buy", "timestamp": 200})
    
    mega = buffer.flush()
    assert len(mega) == 1
    assert mega[0]["price"] == 2005.0
    assert mega[0]["timestamp"] == 200
    assert mega[0]["volume"] == 2.0
