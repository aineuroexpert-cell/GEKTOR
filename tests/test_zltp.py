# tests/test_zltp.py
"""
[GEKTOR v21.66] Zero-Latency Telemetry Pipeline — Stress Test Suite.

Tests:
  1. Ring Buffer correctness (push/pop, wrap-around, CRC integrity)
  2. Storm-adaptive sampling (rate detection, back-pressure)
  3. Full pipeline integration (Ring → Writer → Disk)
  4. Overrun behavior (producer faster than consumer)
  5. Corruption recovery (damaged entries skipped)
"""

import time
import struct
import zlib
import pytest
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from skills.gerald_sniper.core.realtime.log_ring import (
    SPSCLogRingBuffer, HEADER_SIZE, ENTRY_HEADER_FMT, ENTRY_HEADER_SIZE,
    ENTRY_FOOTER_SIZE, LEVEL_INFO, LEVEL_WARNING, LEVEL_ERROR, LEVEL_CRITICAL,
    MSG_MAX_LEN,
)
from skills.gerald_sniper.core.realtime.log_sink import (
    ZeroLatencyLogSink, StormDetector,
)


class TestSPSCLogRingBuffer:
    """Ring Buffer unit tests."""
    
    def setup_method(self):
        """Create a fresh ring buffer for each test."""
        self.ring_name = f"test_ring_{os.getpid()}_{id(self)}"
        self.ring = SPSCLogRingBuffer.create(capacity_mb=1, name=self.ring_name)
    
    def teardown_method(self):
        """Clean up shared memory."""
        try:
            self.ring.destroy()
        except Exception:
            pass
    
    def test_push_pop_single_entry(self):
        """Basic push/pop roundtrip."""
        msg = b"Hello GEKTOR"
        assert self.ring.push(LEVEL_INFO, msg) is True
        
        # Attach as consumer
        consumer = SPSCLogRingBuffer.attach(self.ring_name)
        entry = consumer.pop()
        
        assert entry is not None
        level, ts_ns, data = entry
        assert level == LEVEL_INFO
        assert data == msg
        assert ts_ns > 0
        
        consumer.close()
    
    def test_push_pop_multiple_entries(self):
        """Multiple entries maintain FIFO order."""
        messages = [f"msg_{i}".encode() for i in range(100)]
        
        for i, msg in enumerate(messages):
            assert self.ring.push(LEVEL_WARNING, msg) is True
        
        consumer = SPSCLogRingBuffer.attach(self.ring_name)
        
        for i, expected_msg in enumerate(messages):
            entry = consumer.pop()
            assert entry is not None, f"Entry {i} was None"
            level, ts_ns, data = entry
            assert data == expected_msg, f"Entry {i}: {data} != {expected_msg}"
            assert level == LEVEL_WARNING
        
        # Buffer should be empty now
        assert consumer.pop() is None
        consumer.close()
    
    def test_empty_pop_returns_none(self):
        """Pop from empty ring returns None."""
        consumer = SPSCLogRingBuffer.attach(self.ring_name)
        assert consumer.pop() is None
        consumer.close()
    
    def test_message_truncation(self):
        """Oversized messages are truncated, not rejected."""
        huge_msg = b"X" * (MSG_MAX_LEN + 500)
        assert self.ring.push(LEVEL_INFO, huge_msg) is True
        
        consumer = SPSCLogRingBuffer.attach(self.ring_name)
        entry = consumer.pop()
        assert entry is not None
        _, _, data = entry
        assert len(data) <= MSG_MAX_LEN
        assert data.endswith(b"...")
        consumer.close()
    
    def test_batch_pop(self):
        """pop_batch returns multiple entries at once."""
        for i in range(50):
            self.ring.push(LEVEL_INFO, f"batch_{i}".encode())
        
        consumer = SPSCLogRingBuffer.attach(self.ring_name)
        batch = consumer.pop_batch(max_entries=100)
        
        assert len(batch) == 50
        assert batch[0][2] == b"batch_0"
        assert batch[49][2] == b"batch_49"
        consumer.close()
    
    def test_crc_integrity(self):
        """CRC32 corruption detection works."""
        msg = b"critical trade signal"
        self.ring.push(LEVEL_CRITICAL, msg)
        
        # Corrupt a byte in the data region (after header)
        corrupt_offset = HEADER_SIZE + ENTRY_HEADER_SIZE + 5
        original_byte = self.ring._shm.buf[corrupt_offset]
        self.ring._shm.buf[corrupt_offset] = (original_byte + 1) % 256
        
        consumer = SPSCLogRingBuffer.attach(self.ring_name)
        entry = consumer.pop()
        
        # Corrupted entry should be skipped (returns None)
        assert entry is None
        consumer.close()
    
    def test_stats_reporting(self):
        """Stats reflect buffer state correctly."""
        stats_before = self.ring.stats
        assert stats_before["total_written"] == 0
        assert stats_before["fill_pct"] == 0.0
        
        for i in range(10):
            self.ring.push(LEVEL_INFO, b"test")
        
        stats_after = self.ring.stats
        assert stats_after["total_written"] == 10
        assert stats_after["fill_pct"] > 0.0
    
    def test_overrun_counter(self):
        """When buffer is full, overrun counter increments."""
        # Fill the 1MB buffer with large messages until it can't fit more
        large_msg = b"X" * 2000
        push_count = 0
        drop_count = 0
        
        for _ in range(10_000):
            result = self.ring.push(LEVEL_INFO, large_msg)
            if result:
                push_count += 1
            else:
                drop_count += 1
                if drop_count >= 10:
                    break
        
        stats = self.ring.stats
        assert stats["overrun_count"] > 0 or stats["total_dropped"] > 0
        assert push_count > 0


class TestStormDetector:
    """Storm-adaptive sampling tests."""
    
    def test_normal_mode_always_emits(self):
        """Below threshold, all messages pass through."""
        detector = StormDetector(storm_threshold=1000)
        
        # 100 messages should all pass
        results = [detector.tick() for _ in range(100)]
        assert all(results)
    
    def test_storm_mode_samples(self):
        """Above threshold, sampling kicks in."""
        detector = StormDetector(storm_threshold=100, sample_ratio=10)
        
        # Force EMA above threshold
        detector._rate_ema = 200.0
        
        results = [detector.tick() for _ in range(100)]
        emitted = sum(results)
        
        # Should emit roughly 10% (100/10 = 10)
        assert 5 <= emitted <= 15, f"Expected ~10 emitted, got {emitted}"
    
    def test_is_storm_flag(self):
        """is_storm reflects current rate."""
        detector = StormDetector(storm_threshold=500)
        assert not detector.is_storm
        
        detector._rate_ema = 600.0
        assert detector.is_storm


class TestZeroLatencyLogSink:
    """Sink integration tests."""
    
    def setup_method(self):
        self.ring_name = f"test_sink_{os.getpid()}_{id(self)}"
        self.ring = SPSCLogRingBuffer.create(capacity_mb=1, name=self.ring_name)
        self.sink = ZeroLatencyLogSink(self.ring, emergency_stderr=False)
    
    def teardown_method(self):
        try:
            self.ring.destroy()
        except Exception:
            pass
    
    def test_diagnostics(self):
        """Diagnostics return valid structure."""
        diag = self.sink.diagnostics
        assert "total_emitted" in diag
        assert "storm_active" in diag
        assert "ring_stats" in diag
        assert diag["total_emitted"] == 0


class TestLatencyBenchmark:
    """Micro-benchmark: verify push latency is sub-microsecond."""
    
    def setup_method(self):
        self.ring_name = f"test_bench_{os.getpid()}_{id(self)}"
        self.ring = SPSCLogRingBuffer.create(capacity_mb=64, name=self.ring_name)
    
    def teardown_method(self):
        try:
            self.ring.destroy()
        except Exception:
            pass
    
    def test_push_latency_under_1_microsecond(self):
        """
        Single push should complete in under 1μs on modern hardware.
        We allow 5μs as a safe margin for CI environments.
        """
        msg = b"VPIN spike: 0.87 | SOL/USDT"
        
        # Warmup
        for _ in range(1000):
            self.ring.push(LEVEL_INFO, msg)
        
        # Benchmark
        iterations = 10_000
        start = time.perf_counter_ns()
        
        for _ in range(iterations):
            self.ring.push(LEVEL_INFO, msg)
        
        elapsed_ns = time.perf_counter_ns() - start
        avg_ns = elapsed_ns / iterations
        
        print(f"\n📊 Push latency: {avg_ns:.0f}ns avg ({avg_ns/1000:.2f}μs)")
        
        # Assert: must be under 5μs (generous margin for CI)
        assert avg_ns < 5_000, f"Push latency {avg_ns}ns exceeds 5μs threshold"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
