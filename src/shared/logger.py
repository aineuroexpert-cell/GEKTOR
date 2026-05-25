import sys
import os
from loguru import logger
from src.shared.config import config
import re

SENSITIVE_PATTERNS = [
    (re.compile(r'bot\d{8,}:[A-Za-z0-9_-]{30,}'), 'bot***:***TOKEN***'),
    (re.compile(r'Bearer [A-Za-z0-9._-]+'), 'Bearer ***MASKED***'),
    (re.compile(r'api[_-]?key["\s:=]+["\']?[A-Za-z0-9_-]+'), 'api_key=***MASKED***'),
]

def mask_sensitive(message: str) -> str:
    result = str(message)
    for pattern, replacement in SENSITIVE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result

class SensitiveFilter:
    def __call__(self, record):
        record["message"] = mask_sensitive(record["message"])
        if record.get("exception"):
            exc = record["exception"]
            if exc and hasattr(exc, 'value') and exc.value:
                exc_str = str(exc.value)
                masked = mask_sensitive(exc_str)
                if masked != exc_str:
                    record["message"] += f"\\n(Exception masked: {masked})"
        return True

def setup_logger():
    # Remove default handler
    logger.remove()

    # Console handler
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=config.system.log_level if config else "INFO",
        filter=SensitiveFilter(),
    )

    # File handler with rotation
    log_path = config.paths.logs if config else "logs/gerald_sys.log"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger.add(
        log_path,
        rotation="10 MB",
        retention="10 days",
        compression="zip",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=config.system.log_level if config else "INFO",
        filter=SensitiveFilter(),
    )


# Initialize logger
setup_logger()
