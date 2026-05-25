import os
import json
import asyncio
from loguru import logger

class VaultInjector:
    """
    [GEKTOR v21.71] Zero-Disk Footprint Secret Injection.
    Secrets are received only via a RAM-disk Named Pipe (FIFO) 
    and exist exclusively in the process memory.
    """
    def __init__(self, ramdisk_path: str = "/mnt/ramdisk/gektor_secrets.fifo"):
        self.fifo_path = ramdisk_path

    def _read_fifo_blocking(self) -> dict:
        """Runs in a ThreadPool to prevent blocking the asyncio Event Loop."""
        # Create FIFO pipe in RAM-disk
        if not os.path.exists(self.fifo_path):
            try:
                os.mkfifo(self.fifo_path)
                # Restrict permissions to owner only
                os.chmod(self.fifo_path, 0o600)
            except Exception as e:
                logger.error(f"Failed to create FIFO: {e}")
                # Fallback for Windows/Testing
                pass
        
        logger.info(f"🛡️ [VAULT] AWAITING DECRYPTION KEY IN RAM-DISK: {self.fifo_path}")
        
        try:
            # open() blocks the thread until a Writer connects
            with open(self.fifo_path, 'r') as fifo:
                raw_data = fifo.read()
                
            # Destroy the bridge immediately after reading
            if os.path.exists(self.fifo_path):
                os.unlink(self.fifo_path)
            logger.success("🔑 [VAULT] SECRETS INJECTED. FIFO DESTROYED.")
            
            return json.loads(raw_data)
        except Exception as e:
            logger.critical(f"💥 [VAULT] Injection Failed: {e}")
            raise

    async def wait_for_secrets(self) -> dict:
        """
        Asynchronously waits for secrets without blocking the Event Loop.
        If on Windows (no mkfifo), it will just return empty or test mock.
        """
        if os.name == 'nt':
            logger.warning("⚠️ [VAULT] Windows detected. Bypassing FIFO injection for local development.")
            return {} # Returning empty allows local .env fallback for testing

        loop = asyncio.get_running_loop()
        secrets = await loop.run_in_executor(None, self._read_fifo_blocking)
        return secrets
