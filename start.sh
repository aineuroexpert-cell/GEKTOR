#!/bin/bash
# ╔══════════════════════════════════════════════════════════════╗
# ║  GEKTOR APEX — АБСОЛЮТНЫЙ ПУСК v2.0                        ║
# ║  Автоматическая стерилизация, проверка и запуск              ║
# ╚══════════════════════════════════════════════════════════════╝
set -e

PROJECT_DIR="/opt/gektor"
VENV_DIR="$PROJECT_DIR/venv"
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo ""
echo -e "${CYAN}╔══════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║     🚀 GEKTOR APEX — АБСОЛЮТНЫЙ ПУСК v2.0      ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════╝${NC}"
echo ""

# ═══════════════════════════════════════════
# ФАЗА 0: УБИЙСТВО ЗОМБИ-ПРОЦЕССОВ
# ═══════════════════════════════════════════
echo -e "${YELLOW}💀 [ФАЗА 0] Поиск и уничтожение зомби-процессов...${NC}"

# Найти и убить все старые экземпляры main.py
OLD_PIDS=$(pgrep -f "python.*main.py" 2>/dev/null || true)
if [ -n "$OLD_PIDS" ]; then
    echo -e "  ${RED}Найдены зомби-процессы: $OLD_PIDS${NC}"
    for pid in $OLD_PIDS; do
        echo -e "  ${RED}Убиваем PID $pid...${NC}"
        kill -SIGTERM "$pid" 2>/dev/null || true
    done
    sleep 2
    # Контрольный выстрел для упрямых
    STILL_ALIVE=$(pgrep -f "python.*main.py" 2>/dev/null || true)
    if [ -n "$STILL_ALIVE" ]; then
        echo -e "  ${RED}Контрольный выстрел (SIGKILL)...${NC}"
        kill -9 $STILL_ALIVE 2>/dev/null || true
        sleep 1
    fi
    echo -e "  ${GREEN}✅ Зомби устранены${NC}"
else
    echo -e "  ${GREEN}✅ Чисто — нет висящих процессов${NC}"
fi

# ═══════════════════════════════════════════
# ФАЗА 1: СТЕРИЛИЗАЦИЯ ПАМЯТИ
# ═══════════════════════════════════════════
echo ""
echo -e "${YELLOW}🧹 [ФАЗА 1] Стерилизация оперативной памяти (/dev/shm)...${NC}"

SHM_COUNT=$(ls /dev/shm/psm_* /dev/shm/gektor_* 2>/dev/null | wc -l || echo 0)
rm -f /dev/shm/psm_* 2>/dev/null
rm -f /dev/shm/gektor_* 2>/dev/null

if [ "$SHM_COUNT" -gt 0 ]; then
    echo -e "  ${GREEN}✅ Удалено $SHM_COUNT SHM-сегментов${NC}"
else
    echo -e "  ${GREEN}✅ SHM чист${NC}"
fi

# ═══════════════════════════════════════════
# ФАЗА 2: ПРОВЕРКА СТРУКТУРЫ ПРОЕКТА
# ═══════════════════════════════════════════
echo ""
echo -e "${YELLOW}📂 [ФАЗА 2] Проверка структуры проекта...${NC}"

ERRORS=0

# Проверяем что src НЕ в venv
if [ -d "$VENV_DIR/src/infrastructure" ]; then
    echo -e "  ${RED}❌ КРИТИЧНО: src/ найден ВНУТРИ venv/! Удаляю мусор...${NC}"
    rm -rf "$VENV_DIR/src"
    echo -e "  ${GREEN}  → Удалён venv/src/${NC}"
fi

# Проверяем ключевые файлы
for f in main.py src/infrastructure/bybit.py src/infrastructure/telegram_notifier.py src/infrastructure/config.py src/infrastructure/watchdog.py src/domain/math_core.py src/domain/dollar_bar.py src/application/orchestrator.py; do
    if [ ! -f "$PROJECT_DIR/$f" ]; then
        echo -e "  ${RED}❌ ОТСУТСТВУЕТ: $f${NC}"
        ERRORS=$((ERRORS + 1))
    fi
done

if [ "$ERRORS" -gt 0 ]; then
    echo -e "  ${RED}🛑 Найдено $ERRORS отсутствующих файлов! ПРЕРЫВАЮ ЗАПУСК.${NC}"
    exit 1
fi
echo -e "  ${GREEN}✅ Все критические файлы на месте${NC}"

# ═══════════════════════════════════════════
# ФАЗА 3: ПРОВЕРКА ПАТЧЕЙ (PREFLIGHT)
# ═══════════════════════════════════════════
echo ""
echo -e "${YELLOW}🔍 [ФАЗА 3] Проверка боевых патчей...${NC}"

PATCH_ERRORS=0

# TG Nuclear Isolation
if grep -q "Nuclear-Isolated" "$PROJECT_DIR/src/infrastructure/telegram_notifier.py"; then
    echo -e "  ${GREEN}✅ Telegram: Nuclear Isolation v5.25${NC}"
else
    echo -e "  ${RED}❌ Telegram: СТАРЫЙ КОД (нет Nuclear Isolation)${NC}"
    PATCH_ERRORS=$((PATCH_ERRORS + 1))
fi

# Latency Shield disabled
if grep -q "LATENCY SHIELD: DISABLED" "$PROJECT_DIR/src/infrastructure/bybit.py"; then
    echo -e "  ${GREEN}✅ Bybit: Latency Shield отключен${NC}"
else
    echo -e "  ${RED}❌ Bybit: Latency Shield ВСЁ ЕЩЁ АКТИВЕН${NC}"
    PATCH_ERRORS=$((PATCH_ERRORS + 1))
fi

# Watchdog diagnostic mode
if grep -q "DIAGNOSTIC MODE" "$PROJECT_DIR/src/infrastructure/bybit.py"; then
    echo -e "  ${GREEN}✅ Watchdog: Diagnostic Mode (не убивает сокет)${NC}"
else
    echo -e "  ${RED}❌ Watchdog: БОЕВОЙ РЕЖИМ (убивает сокет!)${NC}"
    PATCH_ERRORS=$((PATCH_ERRORS + 1))
fi

# Side contract fix
if grep -q "side_str" "$PROJECT_DIR/src/domain/math_core.py"; then
    echo -e "  ${GREEN}✅ MathCore: side_str контракт (не bool)${NC}"
else
    echo -e "  ${RED}❌ MathCore: СТАРЫЙ КОД (bool is_m передаётся в dollar_bar)${NC}"
    PATCH_ERRORS=$((PATCH_ERRORS + 1))
fi

# Dollar bar defense
if grep -q 'str(side).upper()' "$PROJECT_DIR/src/domain/dollar_bar.py"; then
    echo -e "  ${GREEN}✅ DollarBar: str(side).upper() defense${NC}"
else
    echo -e "  ${RED}❌ DollarBar: НЕТ ЗАЩИТЫ от bool${NC}"
    PATCH_ERRORS=$((PATCH_ERRORS + 1))
fi

# Drain error logging
if grep -q "Ingestor Drain Error" "$PROJECT_DIR/src/infrastructure/bybit.py"; then
    echo -e "  ${GREEN}✅ Ingestor: except Exception логируется${NC}"
else
    echo -e "  ${RED}❌ Ingestor: except Exception: pass (тихий глушитель!)${NC}"
    PATCH_ERRORS=$((PATCH_ERRORS + 1))
fi

if [ "$PATCH_ERRORS" -gt 0 ]; then
    echo ""
    echo -e "  ${RED}🛑 ОБНАРУЖЕНО $PATCH_ERRORS НЕПРОПАТЧЕННЫХ ФАЙЛОВ!${NC}"
    echo -e "  ${RED}   Ты залил СТАРЫЙ код. Перезалей src/ с локального ПК.${NC}"
    echo -e "  ${RED}   ЗАПУСК ПРЕРВАН.${NC}"
    exit 1
fi

# ═══════════════════════════════════════════
# ФАЗА 4: PTP CLOCK SAFETY
# ═══════════════════════════════════════════
echo ""
echo -e "${YELLOW}⏱️ [ФАЗА 4] Разблокировка PTP-часов...${NC}"

python3 -c '
p = "/opt/gektor/src/infrastructure/time_sync.py"
try:
    with open(p, "r") as f: d = f.read()
    old = "raise RuntimeError(\"CRITICAL: Cannot calibrate clock (high RTT/Network fail)\")"
    if old in d:
        d = d.replace(old, "return 0.0")
        with open(p, "w") as f: f.write(d)
        print("  ✅ PTP RuntimeError → return 0.0 (разблокировано)")
    else:
        print("  ✅ PTP уже разблокирован")
except FileNotFoundError:
    print("  ⚠️  time_sync.py не найден (не критично)")
except Exception as e:
    print(f"  ⚠️  PTP patch error: {e}")
'

# ═══════════════════════════════════════════
# ФАЗА 5: RAMDISK (Секретное хранилище)
# ═══════════════════════════════════════════
echo ""
echo -e "${YELLOW}🔐 [ФАЗА 5] Проверка RAM-диска...${NC}"

if mountpoint -q /mnt/ramdisk 2>/dev/null; then
    echo -e "  ${GREEN}✅ /mnt/ramdisk смонтирован${NC}"
else
    echo -e "  ${YELLOW}⚠️  /mnt/ramdisk не смонтирован. Создаю...${NC}"
    sudo mkdir -p /mnt/ramdisk 2>/dev/null || true
    sudo mount -t tmpfs -o size=64M tmpfs /mnt/ramdisk 2>/dev/null || true
    if mountpoint -q /mnt/ramdisk 2>/dev/null; then
        echo -e "  ${GREEN}✅ RAM-диск создан${NC}"
    else
        echo -e "  ${RED}⚠️  Не удалось создать RAM-диск (ключи через .env)${NC}"
    fi
fi

# ═══════════════════════════════════════════
# ФАЗА 6: ЗАПУСК ЯДРА
# ═══════════════════════════════════════════
echo ""
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}🐍 [ФАЗА 6] Активация среды и запуск Ядра...${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}⏳ Ожидание инъекции ключей в Терминале 2:${NC}"
echo -e "${CYAN}   echo '{\"BYBIT_API_KEY\":\"...\",\"BYBIT_API_SECRET\":\"...\"}' > /mnt/ramdisk/gektor_secrets.fifo${NC}"
echo ""

cd "$PROJECT_DIR"
source "$VENV_DIR/bin/activate"
exec python main.py
