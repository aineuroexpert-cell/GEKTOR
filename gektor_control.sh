#!/bin/bash
# ╔══════════════════════════════════════════════════════════════╗
# ║  GEKTOR CONTROL SCRIPT (Среднесрочный Радар)                 ║
# ╚══════════════════════════════════════════════════════════════╝
set -e

ACTION=$1
PROJECT_DIR="/opt/gektor"
SERVICE_NAME="gektor.service"

case "$ACTION" in
    "status")
        echo -e "\e[36m=== GEKTOR SYSTEM HEALTH ===\e[0m"
        systemctl status $SERVICE_NAME --no-pager || true
        echo -e "\n\e[33m=== RAM USAGE ===\e[0m"
        free -h
        echo -e "\n\e[33m=== PORTS & SOCKETS ===\e[0m"
        ss -tuln | grep -E 'State|gektor|python' || true
        echo -e "\n\e[32m=== RECENT LOGS ===\e[0m"
        journalctl -u $SERVICE_NAME -n 20 --no-pager
        ;;
    "clean")
        echo -e "\e[33m=== CLEANING STALE STATE AND CACHES ===\e[0m"
        # Уничтожаем старые снапшоты, чтобы исключить Sequence Gap
        rm -f $PROJECT_DIR/artifacts/spillover.jsonl 2>/dev/null || true
        rm -rf $PROJECT_DIR/__pycache__ 2>/dev/null || true
        # Очистка зомби-памяти Linux (SHM) и семафоров/мьютексов
        rm -f /dev/shm/psm_* 2>/dev/null || true
        rm -f /dev/shm/gektor_* 2>/dev/null || true
        rm -f /dev/shm/sem.gektor_* /dev/shm/sem.psm_* 2>/dev/null || true
        echo -e "\e[32m✅ Clean successful.\e[0m"
        ;;
    "restart")
        echo -e "\e[36m=== ATOMIC RESTART ===\e[0m"
        cd $PROJECT_DIR
        source venv/bin/activate
        echo "Validating Python syntax before ignition..."
        python -m compileall -x "venv" .
        echo "Reloading systemd and restarting..."
        systemctl daemon-reload
        systemctl restart $SERVICE_NAME
        echo -e "\e[32m✅ GEKTOR radar restarted successfully.\e[0m"
        ;;
    *)
        echo "Usage: $0 {status|clean|restart}"
        exit 1
        ;;
esac
