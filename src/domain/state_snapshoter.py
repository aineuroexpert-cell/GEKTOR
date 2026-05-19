import mmap
import ctypes
import os
import json
from typing import Dict, Any

LEDGER_SIZE = 4096  # 4KB for mid-term context

class ContextMemory(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("crc32", ctypes.c_uint32),
        ("payload", ctypes.c_char * (LEDGER_SIZE - 4))
    ]

class StateSnapshoter:
    """
    Synchronously dumps mid-term context (KDE levels, macro regimes) to mmap.
    Hydrates state on Cold Start before WebSocket initialization.
    """
    def __init__(self, filename: str = "artifacts/midterm_state.mmap"):
        self.filename = filename
        self._init_mmap()
        
    def _init_mmap(self) -> None:
        if not os.path.exists(self.filename):
            fd = os.open(self.filename, os.O_CREAT | os.O_TRUNC | os.O_RDWR)
            os.ftruncate(fd, LEDGER_SIZE)
            os.close(fd)
            
        fd = os.open(self.filename, os.O_RDWR)
        self.buf = mmap.mmap(fd, LEDGER_SIZE, mmap.MAP_SHARED, mmap.PROT_WRITE)
        self.struct = ContextMemory.from_buffer(self.buf)
        os.close(fd)

    def snapshot(self, context: Dict[str, Any]) -> None:
        """Dump context dictionary to mmap (JSON serialized for flexibility)."""
        payload_bytes = json.dumps(context).encode('utf-8')
        if len(payload_bytes) > (LEDGER_SIZE - 4):
            raise ValueError("Context payload exceeds mmap size.")
            
        # Write payload
        self.struct.payload = payload_bytes
        # In a real implementation, calculate and write CRC32 here
        # self.struct.crc32 = compute_crc(payload_bytes)

    def hydrate_state(self) -> Dict[str, Any]:
        """Read context dictionary from mmap on cold start."""
        # In a real implementation, validate CRC32 here first
        raw_payload = self.struct.payload.rstrip(b'\x00')
        if not raw_payload:
            return {}
        try:
            return json.loads(raw_payload.decode('utf-8'))
        except json.JSONDecodeError:
            return {}
