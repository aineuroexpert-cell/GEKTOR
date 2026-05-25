#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  GEKTOR APEX — PRE-FLIGHT CHECK v1.0                        ║
║  Запусти на сервере ПЕРЕД ./start.sh                         ║
║  python3 preflight_check.py                                  ║
╚══════════════════════════════════════════════════════════════╝
"""
import os
import sys

PASS = "✅"
FAIL = "❌"
WARN = "⚠️"
results = []

def check(name: str, condition: bool, critical: bool = True):
    status = PASS if condition else (FAIL if critical else WARN)
    results.append((status, name, critical and not condition))
    print(f"  {status} {name}")
    return condition

def file_contains(filepath: str, marker: str) -> bool:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return marker in f.read()
    except FileNotFoundError:
        return False

def file_not_contains(filepath: str, marker: str) -> bool:
    """Returns True if file does NOT contain marker (i.e. old code is gone)."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return marker not in f.read()
    except FileNotFoundError:
        return False

# ─── Detect project root ───
script_dir = os.path.dirname(os.path.abspath(__file__))
if os.path.exists(os.path.join(script_dir, "main.py")):
    ROOT = script_dir
elif os.path.exists("/opt/gektor/main.py"):
    ROOT = "/opt/gektor"
else:
    print(f"{FAIL} Не могу найти корень проекта! Запусти из директории с main.py")
    sys.exit(1)

SRC = os.path.join(ROOT, "src")
INF = os.path.join(SRC, "infrastructure")
DOM = os.path.join(SRC, "domain")
APP = os.path.join(SRC, "application")

print("=" * 60)
print("  GEKTOR APEX — PRE-FLIGHT DIAGNOSTICS")
print(f"  Root: {ROOT}")
print("=" * 60)

# ═══════════════════════════════════════════
# 1. СТРУКТУРА ФАЙЛОВ
# ═══════════════════════════════════════════
print("\n📂 [1/6] СТРУКТУРА ПРОЕКТА")
check("main.py существует", os.path.isfile(os.path.join(ROOT, "main.py")))
check("src/ существует", os.path.isdir(SRC))
check("src/infrastructure/ существует", os.path.isdir(INF))
check("src/domain/ существует", os.path.isdir(DOM))
check("src/application/ существует", os.path.isdir(APP))

# КРИТИЧНО: src НЕ должен быть внутри venv
venv_src = os.path.join(ROOT, "venv", "src")
check("src/ НЕ внутри venv/ (нет дубликата)", not os.path.isdir(venv_src))

# Ключевые файлы
key_files = [
    ("bybit.py", os.path.join(INF, "bybit.py")),
    ("telegram_notifier.py", os.path.join(INF, "telegram_notifier.py")),
    ("watchdog.py", os.path.join(INF, "watchdog.py")),
    ("config.py", os.path.join(INF, "config.py")),
    ("math_core.py", os.path.join(DOM, "math_core.py")),
    ("dollar_bar.py", os.path.join(DOM, "dollar_bar.py")),
    ("radar_pipeline.py", os.path.join(APP, "radar_pipeline.py")),
    ("outbox_relay.py", os.path.join(APP, "outbox_relay.py")),
    ("watchdog.py", os.path.join(APP, "watchdog.py")),
]
for name, path in key_files:
    check(f"{name} существует", os.path.isfile(path))

# ═══════════════════════════════════════════
# 2. ПАТЧ: TELEGRAM NUCLEAR ISOLATION
# ═══════════════════════════════════════════
print("\n🛡️ [2/6] TELEGRAM NUCLEAR ISOLATION")
tg_file = os.path.join(INF, "telegram_notifier.py")
check("Nuclear-Isolated Egress (v5.25)",
      file_contains(tg_file, "Nuclear-Isolated"))
check("ConnectionRefusedError перехватывается",
      file_contains(tg_file, "ConnectionRefusedError"))
check("Session creation fallback (retry 10s)",
      file_contains(tg_file, "Session creation failed"))
check("asyncio.timeout(3.0) на dispatch",
      file_contains(tg_file, "asyncio.timeout(3.0)"))

# ═══════════════════════════════════════════
# 3. ПАТЧ: BYBIT INGESTOR
# ═══════════════════════════════════════════
print("\n📡 [3/6] BYBIT INGESTOR (Latency Shield & Watchdog)")
bybit_file = os.path.join(INF, "bybit.py")
check("Latency Shield ОТКЛЮЧЕН",
      file_contains(bybit_file, "LATENCY SHIELD: DISABLED"))
check("Watchdog в DIAGNOSTIC MODE (не убивает сокет)",
      file_contains(bybit_file, "DIAGNOSTIC MODE"))
check("except Exception: pass УДАЛЁН (есть логирование)",
      file_contains(bybit_file, "Ingestor Drain Error"))
check("Orderbook дельты обновляют last_tick_wall",
      file_contains(bybit_file, "Orderbook deltas feed watchdog"))
check("UNHANDLED TOPIC логируется",
      file_contains(bybit_file, "UNHANDLED TOPIC"))
check("last_tick_wall инициализирован в __init__",
      file_contains(bybit_file, "self.last_tick_wall = time.monotonic()"))
check("RAW TRAFFIC диагностика",
      file_contains(bybit_file, "RAW TRAFFIC"))

# ═══════════════════════════════════════════
# 4. ПАТЧ: WATCHDOG EVACUATION DISABLED
# ═══════════════════════════════════════════
print("\n🚨 [4/6] WATCHDOG EVACUATION")
wd_file = os.path.join(INF, "watchdog.py")
check("Эвакуация ОТКЛЮЧЕНА (diagnostic mode)",
      file_contains(wd_file, "Evacuation DISABLED"))
check("cancel-all НЕ вызывается (create_task закомментирован)",
      file_contains(wd_file, "# asyncio.create_task(self._execute_out_of_band_cancel"))

# ═══════════════════════════════════════════
# 5. ПАТЧ: SIDE CONTRACT (bool → str)
# ═══════════════════════════════════════════
print("\n🔧 [5/6] SIDE CONTRACT (bool → str fix)")
mc_file = os.path.join(DOM, "math_core.py")
db_file = os.path.join(DOM, "dollar_bar.py")
check("math_core: side_str вместо is_m",
      file_contains(mc_file, "side_str"))
check("math_core: generator.process_tick(p, v, side_str, ts)",
      file_contains(mc_file, "side_str, ts"))
check("dollar_bar: str(side).upper()",
      file_contains(db_file, "str(side).upper()"))

# ═══════════════════════════════════════════
# 6. КОНФИГ & СРЕДА
# ═══════════════════════════════════════════
print("\n⚙️ [6/6] КОНФИГУРАЦИЯ & СРЕДА")
cfg_file = os.path.join(INF, "config.py")
check("Taiwan proxy настроен (socks5)",
      file_contains(cfg_file, "socks5://"))

# SHM check (Linux only)
if sys.platform == "linux":
    shm_files = [f for f in os.listdir("/dev/shm") if f.startswith(("psm_", "gektor_"))]
    check(f"SHM чист (нет зомби-сегментов)", len(shm_files) == 0, critical=False)
    if shm_files:
        print(f"     Зомби: {shm_files}")
        print(f"     Выполни: rm -f /dev/shm/psm_* /dev/shm/gektor_*")

# venv check
venv_path = os.path.join(ROOT, "venv")
if os.path.isdir(venv_path):
    check("venv/ существует", True, critical=False)
    # Check Python version in venv
    venv_python = os.path.join(venv_path, "bin", "python3")
    if os.path.isfile(venv_python):
        check("venv/bin/python3 существует", True, critical=False)


# Helper Functions for Physical and Network tests
def _verify_shm_layout() -> bool:
    import ctypes
    import time
    try:
        sys.path.insert(0, ROOT)
        from src.infrastructure.shm_layout import SHMOrderBook, SHMLevel
    except ImportError as e:
        print(f"     ❌ Не удалось импортировать shm_layout: {e}")
        return False
    
    # 1. Structural checks
    actual_level_size = ctypes.sizeof(SHMLevel)
    expected_level_size = 16
    if actual_level_size != expected_level_size:
        print(f"     ❌ SHMLevel size mismatch: {actual_level_size} != {expected_level_size}")
        return False
        
    actual_book_size = ctypes.sizeof(SHMOrderBook)
    expected_book_size = 1624
    if actual_book_size != expected_book_size:
        print(f"     ❌ SHMOrderBook size mismatch: {actual_book_size} != {expected_book_size}")
        return False
        
    # Verify offsets
    epoch_offset = ctypes.offsetof(SHMOrderBook, "epoch")
    bids_offset = ctypes.offsetof(SHMOrderBook, "bids")
    asks_offset = ctypes.offsetof(SHMOrderBook, "asks")
    if epoch_offset != 0 or bids_offset != 24 or asks_offset != 824:
        print(f"     ❌ SHMOrderBook field offset mismatch (epoch: {epoch_offset}, bids: {bids_offset}, asks: {asks_offset})")
        return False
        
    # 2. Performance benchmark
    book = SHMOrderBook()
    mv = memoryview(book)
    
    iters = 100_000
    t0 = time.perf_counter_ns()
    for _ in range(iters):
        _ = mv[24]
    t1 = time.perf_counter_ns()
    
    avg_lat_ns = (t1 - t0) / iters
    print(f"     📊 Средняя задержка чтения SHM: {avg_lat_ns:.2f} ns")
    if avg_lat_ns > 50.0:
        print(f"     ❌ Задержка превышает 50ns: {avg_lat_ns:.2f} ns")
        return False
    return True

def _verify_ipc_latency() -> bool:
    import ctypes
    import mmap
    import os
    import time
    import threading
    try:
        sys.path.insert(0, ROOT)
        from src.infrastructure.ipc import SpinlockMemory
    except ImportError as e:
        print(f"     ❌ Не удалось импортировать SpinlockMemory из ipc: {e}")
        return False
        
    mmap_size = 1024 * 1024
    if sys.platform != "win32":
        shm_file = "/dev/shm/gektor_preflight_ipc"
    else:
        shm_file = "gektor_preflight_ipc.tmp"
        
    try:
        fd = os.open(shm_file, os.O_CREAT | os.O_TRUNC | os.O_RDWR)
        os.ftruncate(fd, mmap_size)
        if sys.platform != "win32":
            buf = mmap.mmap(fd, mmap_size, mmap.MAP_SHARED, mmap.PROT_WRITE)
        else:
            buf = mmap.mmap(fd, mmap_size, access=mmap.ACCESS_WRITE)
    except Exception as e:
        print(f"     ⚠️ Не удалось создать тестовый mmap сегмент: {e}")
        return False
        
    try:
        state = SpinlockMemory.from_buffer(buf)
        state.data_ready = 0
        state.latest_u_id = 0
        
        iterations = 5000
        
        def writer_loop():
            for i in range(iterations):
                while state.data_ready != 0:
                    pass
                state.latest_u_id = i
                state.data_ready = 1
                
        t0 = time.perf_counter()
        t = threading.Thread(target=writer_loop)
        t.start()
        
        for i in range(iterations):
            while state.data_ready != 1:
                pass
            _ = state.latest_u_id
            state.data_ready = 0
            
        t.join()
        t1 = time.perf_counter()
        
        total_time_us = (t1 - t0) * 1_000_000
        avg_rtt_us = total_time_us / iterations
        print(f"     📊 Средний RTT IPC Spinlock: {avg_rtt_us:.2f} мкс (iters: {iterations})")
        if avg_rtt_us > 10.0:
            print(f"     ❌ Задержка IPC превышает 10 мкс: {avg_rtt_us:.2f} мкс")
            return False
        return True
    finally:
        buf.close()
        os.close(fd)
        try:
            os.remove(shm_file)
        except Exception:
            pass

def _check_exchange_reachability() -> bool:
    import socket
    import ssl
    import time
    
    endpoints = ["api.bybit.com", "stream.bybit.com"]
    success = True
    
    for host in endpoints:
        try:
            ip = socket.gethostbyname(host)
        except socket.gaierror:
            print(f"     ❌ Ошибка резолва DNS для {host}")
            return False
            
        try:
            t0 = time.perf_counter()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, 443))
            t_connect = time.perf_counter()
            tcp_rtt_ms = (t_connect - t0) * 1000
            
            context = ssl.create_default_context()
            tls_sock = context.wrap_socket(sock, server_hostname=host)
            t_handshake = time.perf_counter()
            tls_time_ms = (t_handshake - t_connect) * 1000
            
            tls_sock.close()
            sock.close()
            
            print(f"     📶 {host} ({ip}): TCP RTT={tcp_rtt_ms:.2f}ms, TLS Handshake={tls_time_ms:.2f}ms")
            if tcp_rtt_ms > 200.0:
                print(f"     ⚠️ Внимание: Высокий пинг ({tcp_rtt_ms:.2f}ms)")
        except Exception as e:
            print(f"     ❌ Соединение с {host} прервано: {e}")
            success = False
    return success


# ═══════════════════════════════════════════
# 7. ФИЗИЧЕСКИЕ И СЕТЕВЫЕ ПРОВЕРКИ HFT
# ═══════════════════════════════════════════
print("\n⚡ [7/6] ФИЗИЧЕСКИЕ И СЕТЕВЫЕ ТЕСТЫ HFT")
check("SHM Layout и латентность чтения < 50нс", _verify_shm_layout())
check("IPC Spinlock латентность < 10мкс", _verify_ipc_latency())
check("Bybit Exchange доступность и TLS Handshake", _check_exchange_reachability())


# ═══════════════════════════════════════════
# ИТОГ
# ═══════════════════════════════════════════
print("\n" + "=" * 60)
critical_fails = sum(1 for s, _, is_crit in results if is_crit)
total_pass = sum(1 for s, _, _ in results if s == PASS)
total_warn = sum(1 for s, _, _ in results if s == WARN)
total_fail = sum(1 for s, _, _ in results if s == FAIL)

print(f"  ИТОГО: {total_pass} {PASS}  |  {total_warn} {WARN}  |  {total_fail} {FAIL}")

if critical_fails == 0:
    print(f"\n  🚀 СИСТЕМА ГОТОВА К ЗАПУСКУ. Выполняй ./start.sh")
else:
    print(f"\n  🛑 ОБНАРУЖЕНО {critical_fails} КРИТИЧЕСКИХ ПРОБЛЕМ!")
    print(f"  НЕ ЗАПУСКАЙ систему до устранения.")
    for status, name, is_crit in results:
        if is_crit:
            print(f"     {FAIL} {name}")

print("=" * 60)
sys.exit(1 if critical_fails > 0 else 0)
