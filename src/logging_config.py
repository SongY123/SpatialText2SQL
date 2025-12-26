"""Central logging configuration for the `src` package.

This module configures and exposes a package-level `logger` object that other
modules can import directly:

    from src.logging_config import logger
    logger.info("message")

API:
- setup_logging(level=logging.INFO, fmt=None, force=False): configure the
  package logger (idempotent unless force=True).
- get_logger(name=None): get a child logger under the package (helpful for
  module-level `__name__` style usage: `get_logger(__name__)`).
- logger: the configured top-level `src` logger (module-level convenience).

Design constraints:
- Don't clobber existing handlers by default (respect app-level logging).
- Provide `force=True` to reconfigure when necessary (e.g., tests).
"""
from __future__ import annotations

import logging
from typing import Optional

# Determine the top-level package name (e.g. 'src')
_TOP_NAME = __name__.split('.')[0]


def setup_logging(level: int = logging.INFO, fmt: Optional[str] = None, force: bool = False) -> logging.Logger:
    """Configure the package-level logger and return it.

    Args:
        level: logging level to set on the top-level logger.
        fmt: Optional format string for the StreamHandler. If None a sensible
             default will be used.
        force: If True, will remove existing handlers and reconfigure. Use
               with care; default False to avoid clobbering app-level logging.

    Returns:
        The configured top-level logger instance.
    """
    logger = logging.getLogger(_TOP_NAME)

    # If handlers already exist and force is False, update level and return.
    if logger.handlers and not force:
        logger.setLevel(level)
        return logger

    # Remove existing handlers when forcing reconfiguration.
    if force:
        for h in list(logger.handlers):
            logger.removeHandler(h)

    handler = logging.StreamHandler()
    fmt = fmt or '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)s] - %(message)s'
    handler.setFormatter(logging.Formatter(fmt))
    handler.setLevel(level)

    logger.addHandler(handler)
    logger.setLevel(level)
    # Prevent messages from propagating to the root logger to avoid duplicate output
    logger.propagate = False

    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a logger under the package namespace.

    - get_logger() -> top-level package logger (same as `logger`).
    - get_logger('module') -> logger named 'src.module'
    - get_logger(__name__) -> if __name__ starts with 'src.' returns that logger,
      otherwise returns child under package.
    """
    if not name:
        return logging.getLogger(_TOP_NAME)

    # If passed a full module name that already starts with the package name,
    # return it directly so module-level `__name__` works unchanged.
    if isinstance(name, str) and name.startswith(f"{_TOP_NAME}."):
        return logging.getLogger(name)

    return logging.getLogger(f"{_TOP_NAME}.{name}")


# Configure logger at import time (idempotent). Tests or applications may
# call `setup_logging(..., force=True)` to override this default behavior.
logger: logging.Logger = setup_logging()


__all__ = ["setup_logging", "get_logger", "logger"]
