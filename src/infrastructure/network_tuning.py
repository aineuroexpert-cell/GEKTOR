import socket
from loguru import logger

def apply_hft_socket_tuning(sock: socket.socket) -> None:
    """
    [GEKTOR v11.0] СТЕРИЛИЗАЦИЯ СЕТЕВОГО ТРАКТА.
    Тюнинг сокетов для минимизации латентности и предотвращения Buffer Bloat.
    """
    try:
        # 1. Отключение алгоритма Нейгла (TCP_NODELAY)
        # Мы не копим данные для экономии трафика. Каждый байт должен уйти немедленно.
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        # 2. Раздувание буфера приема (SO_RCVBUF)
        # 16MB для поглощения всплесков во время рыночного шторма (Flash Crash).
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
        
        # 3. Настройка Keep-Alive (Fast Detection)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        
        # 4. (Linux Only) TCP_QUICKACK
        # Отключаем отложенные подтверждения для ускорения протокола
        if hasattr(socket, 'TCP_QUICKACK'):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_QUICKACK, 1)
            
        logger.success("🚀 [NET_TUNING] Socket tuned for HFT (NODELAY=1, RCVBUF=16MB)")
    except Exception as e:
        logger.warning(f"⚠️ [NET_TUNING] Tuning failed or partially applied: {e}")
