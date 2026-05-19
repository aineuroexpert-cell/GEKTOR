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
    ("orchestrator.py", os.path.join(APP, "orchestrator.py")),
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
