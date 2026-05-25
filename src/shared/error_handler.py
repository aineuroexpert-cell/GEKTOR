import sys
import traceback
import asyncio
from typing import Callable
from functools import wraps
from src.shared.logger import logger


class GeraldError(Exception):
    """Base exception class for Gerald-SuperBrain."""

    pass


class HardwareError(GeraldError):
    """Raised when GPU or memory constraints are violated."""

    pass


class ToolExecutionError(GeraldError):
    """Raised when a core tool fails to execute correctly."""

    pass


def safe_async_run(func: Callable):
    """Decorator to catch and log exceptions in async functions."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            err_msg = f"Exception in {func.__name__}: {str(e)}"
            logger.error(err_msg)
            logger.debug(traceback.format_exc())
            return None

    return wrapper


def global_exception_handler(exctype, value, tb):
    """Global handler for uncaught exceptions."""
    if issubclass(exctype, KeyboardInterrupt):
        sys.__excepthook__(exctype, value, tb)
        return

    logger.critical("Uncaught Exception Root Cause Analysis:")
    logger.critical(f"Type: {exctype}")
    logger.critical(f"Value: {value}")
    logger.critical("".join(traceback.format_exception(exctype, value, tb)))


# Set global hook
sys.excepthook = global_exception_handler
