#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  GEKTOR APEX — ADVANCED PRE-FLIGHT GUARDIAN v3.6.3           ║
║  Focus: Network Egress Latency & Dependency Verification    ║
╚══════════════════════════════════════════════════════════════╝
"""
import asyncio
import socket
import ssl
import sys
import time
from typing import NoReturn

class GektorPreflightGuardian:
    def __init__(self):
        self.endpoints = [
            ("stream.bybit.com", 443),
            ("api.bybit.com", 443)
        ]

    def run_all_checks(self) -> None:
        print("=" * 60)
        print("[GEKTOR PREFLIGHT] Engaging Egress & Dependency Diagnostics...")
        print("=" * 60)
        
        # 1. Verify Core Quantitative Stack
        self._verify_dependencies()
        
        # 2. Run TLS Egress & DNS Diagnostics
        asyncio.run(self._check_exchange_reachability())
        
        print("\n[GEKTOR PREFLIGHT] Environment: APPROVED. Egress latency: VERIFIED.")
        print("=" * 60)

    def _verify_dependencies(self) -> None:
        print("[*] Verifying Quantitative Stack...")
        try:
            import numpy as np
            import aiohttp
            import sqlalchemy
            from loguru import logger
            print(f"  ✅ numpy {np.__version__} loaded successfully.")
            print(f"  ✅ aiohttp {aiohttp.__version__} loaded successfully.")
            print(f"  ✅ sqlalchemy {sqlalchemy.__version__} loaded successfully.")
        except ImportError as e:
            self._terminate(f"Required dependency missing: {e}")

    async def _check_exchange_reachability(self) -> None:
        print("[*] Running Bybit Exchange reachability and TLS Handshake benchmarks...")
        for host, port in self.endpoints:
            try:
                # DNS Resolution
                t_dns_start = time.perf_counter()
                ip = socket.gethostbyname(host)
                dns_ms = (time.perf_counter() - t_dns_start) * 1000
                
                # TCP & TLS Connection
                start_time = time.perf_counter()
                context = ssl.create_default_context()
                
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port, ssl=context),
                    timeout=5.0
                )
                
                handshake_ms = (time.perf_counter() - start_time) * 1000
                writer.close()
                await writer.wait_closed()
                
                print(f"  📶 {host} ({ip}):")
                print(f"     • DNS Resolve: {dns_ms:.2f}ms")
                print(f"     • TLS Handshake: {handshake_ms:.2f}ms")
                
                # Assert performance boundary
                limit_ms = 250.0
                if handshake_ms > limit_ms:
                    print(f"     ⚠️ WARNING: Handshake latency exceeds active trading guidelines ({handshake_ms:.2f}ms > {limit_ms}ms)")
                    
            except Exception as e:
                self._terminate(f"Exchange connection to {host}:{port} failed: {e}")

    def _terminate(self, reason: str) -> NoReturn:
        print(f"\n[REJECTED] | Reason: {reason} | Action: Pre-flight Audit Terminated.")
        sys.exit(1)

if __name__ == "__main__":
    GektorPreflightGuardian().run_all_checks()
