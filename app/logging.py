"""Logging setup and interception using Loguru."""

import logging
import sys
from types import FrameType

from loguru import logger


class InterceptHandler(logging.Handler):
    """Intercept standard library logging calls and route them through Loguru."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        frame: FrameType | None = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging(log_level: str = "INFO", log_json: bool = False) -> None:
    """Configure Loguru and intercept standard library logging."""
    logger.remove()

    # Configure default extra so {extra[request_id]} is always present
    logger.configure(extra={"request_id": "-"})

    is_tty = sys.stdout.isatty()

    if log_json:
        logger.add(
            sys.stdout,
            level=log_level.upper(),
            serialize=True,
            enqueue=True,
        )
    else:
        log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "req:<cyan>{extra[request_id]}</cyan> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        )
        logger.add(
            sys.stdout,
            level=log_level.upper(),
            format=log_format,
            colorize=is_tty,
            enqueue=True,
        )

    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "aiohttp", "fastapi"):
        mod_logger = logging.getLogger(logger_name)
        mod_logger.handlers = [InterceptHandler()]
        mod_logger.propagate = False
