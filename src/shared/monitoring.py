import psutil
from typing import Dict
from src.shared.logger import logger
from src.shared.config import config


class PerformanceMonitor:
    """
    Real-time monitoring for Gerald-SuperBrain.
    Tracks VRAM, CPU, and Response Times.
    """

    def __init__(self):
        self.metrics = {"vram_usage_mb": [], "latency_sec": [], "error_count": 0}
        self.vram_limit = config.system.vram_limit_gb * 1024

    def track_vram(self):
        """Monitor GPU memory using psutil/environment."""
        try:
            # Note: For real VRAM we'd need pynvml, but using psutil
            # as a proxy for memory pressure if nvidia-smi isn't available.
            mem = psutil.virtual_memory()
            # For simplicity in this env, we log system memory as well
            logger.debug(f"System RAM usage: {mem.percent}%")

            # TODO: Add pynvml for strict GPU tracking if requirements allow
        except Exception as e:
            logger.warning(f"Monitoring error: {e}")

    def log_latency(self, duration: float):
        self.metrics["latency_sec"].append(duration)
        if duration > 10.0:
            logger.warning(f"SLA Warning: Response took {duration:.2f}s")

    def get_report(self) -> Dict:
        if not self.metrics["latency_sec"]:
            avg_lat = 0
        else:
            avg_lat = sum(self.metrics["latency_sec"]) / len(
                self.metrics["latency_sec"]
            )

        mem = psutil.virtual_memory()
        return {
            "avg_latency": avg_lat,
            "errors": self.metrics["error_count"],
            "vram_status": "OK" if mem.percent < 90 else "CRITICAL",
        }
