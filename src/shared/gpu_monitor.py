import pynvml
import time
from loguru import logger


class GPUMonitor:
    """Hardware monitoring for NVIDIA GPUs using NVML."""

    def __init__(self):
        try:
            pynvml.nvmlInit()
            self.handle = pynvml.nvmlDeviceGetHandleByIndex(0)
            self.enabled = True
        except Exception as e:
            logger.warning(f"GPU Monitor initialization failed: {e}")
            self.enabled = False

    def get_stats(self):
        if not self.enabled:
            return None

        # Default stats
        vram_total = vram_used = vram_free = 0
        gpu_util = mem_util = 0
        temp = 0

        try:
            info = pynvml.nvmlDeviceGetMemoryInfo(self.handle)
            vram_total = info.total / 1024 / 1024
            vram_used = info.used / 1024 / 1024
            vram_free = info.free / 1024 / 1024
        except Exception as e:
            logger.debug(f"Failed to get memory info: {e}")

        try:
            util = pynvml.nvmlDeviceGetUtilizationRates(self.handle)
            gpu_util = util.gpu
            mem_util = util.memory
        except Exception as e:
            logger.debug(f"Failed to get utilization: {e}")

        try:
            temp = pynvml.nvmlDeviceGetTemperature(
                self.handle, pynvml.NVML_TEMPERATURE_GPU
            )
        except Exception as e:
            logger.debug(f"Failed to get temperature: {e}")

        # Only log error if completely missing basic info, and log it sparingly (or avoid logging here to prevent spam)
        if vram_total == 0 and gpu_util == 0 and temp == 0:
            return None

        return {
            "vram_total_mb": vram_total,
            "vram_used_mb": vram_used,
            "vram_free_mb": vram_free,
            "gpu_util_percent": gpu_util,
            "mem_util_percent": mem_util,
            "temp_c": temp,
        }

    def check_safety(self, max_vram_mb=7200, max_temp=82):
        stats = self.get_stats()
        if not stats:
            return True, "No GPU stats"

        if stats["vram_used_mb"] > max_vram_mb:
            return False, f"VRAM Overloaded: {stats['vram_used_mb']:.0f}MB used"

        if stats["temp_c"] > max_temp:
            return False, f"GPU Overheated: {stats['temp_c']}C"

        return True, "GPU Safe"


if __name__ == "__main__":
    monitor = GPUMonitor()
    print("Gerald Hardware Monitor (NVML) - Press Ctrl+C to stop")
    try:
        while True:
            stats = monitor.get_stats()
            if stats:
                print(
                    f"\rGPU: {stats['gpu_util_percent']}% | VRAM: {stats['vram_used_mb']:.0f}MB / {stats['vram_total_mb']:.0f}MB | Temp: {stats['temp_c']}C",
                    end="",
                    flush=True,
                )
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
