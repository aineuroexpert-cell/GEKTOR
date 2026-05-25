import asyncio
import logging

import pytest


# NOTE: The deprecated session-scoped `event_loop` fixture was removed.
# pytest-asyncio 0.23+ with asyncio_mode="auto" manages per-function loops
# automatically. On Python 3.13 the old fixture caused RuntimeError because
# asyncio.get_event_loop() no longer auto-creates a loop outside of async
# context. Each async test now gets its own fresh event loop (function scope),
# which is the correct default for isolated unit tests.


@pytest.fixture(autouse=True)
def silent_logger():
    """Disable logging during tests to keep output clean."""
    logging.getLogger("GEKTOR").setLevel(logging.CRITICAL)
    yield
