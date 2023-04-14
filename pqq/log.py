## based on https://github.com/sanic-org/sanic/blob/main/sanic/log.py
import logging
import sys
from enum import Enum
from typing import Any, Dict
from warnings import warn

LOGGING_CONFIG_DEFAULTS: Dict[str, Any] = dict(  # no cov
    version=1,
    disable_existing_loggers=False,
    loggers={
        "pqq": {"level": "INFO", "handlers": ["console"]},
        "pqq.error": {
            "level": "INFO",
            "handlers": ["error_console"],
            "propagate": True,
            "qualname": "pqq.error",
        },
    },
    handlers={
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "generic",
            "stream": sys.stdout,
        },
        "error_console": {
            "class": "logging.StreamHandler",
            "formatter": "generic",
            "stream": sys.stderr,
        },
    },
    formatters={
        "generic": {
            "format": "%(asctime)s [%(process)s] [%(levelname)s] %(message)s",
            "datefmt": "[%Y-%m-%d %H:%M:%S %z]",
            "class": "logging.Formatter",
        },
    },
)


def is_atty() -> bool:
    return bool(sys.stdout and sys.stdout.isatty())


class VerbosityFilter(logging.Filter):
    verbosity: int = 0

    def filter(self, record: logging.LogRecord) -> bool:
        verbosity = getattr(record, "verbosity", 0)
        return verbosity <= self.verbosity


class Colors(Enum):  # no cov
    END = "\033[0m"
    BOLD = "\033[1m"
    BLUE = "\033[34m"
    GREEN = "\033[32m"
    PURPLE = "\033[35m"
    RED = "\033[31m"
    SANIC = "\033[38;2;255;13;104m"
    YELLOW = "\033[01;33m"


_verbosity_filter = VerbosityFilter()
logger = logging.getLogger("pqq")  # no cov
"""
General pqq logger
"""
# logger.addFilter(_verbosity_filter)

error_logger = logging.getLogger("pqq.error")  # no cov
"""
Logger used by Sanic for error logging
"""
error_logger.addFilter(_verbosity_filter)


def deprecation(message: str, version: float):  # no cov
    """
    Add a deprecation notice
    Example when a feature is being removed. In this case, version
    should be AT LEAST next version + 2
        deprecation("Helpful message", 99.9)
    Example when a feature is deprecated but not being removed:
        deprecation("Helpful message", 0)
    :param message: The message of the notice
    :type message: str
    :param version: The version when the feature will be removed. If it is
      not being removed, then set version=0.
    :type version: float
    """
    version_display = f" v{version}" if version else ""
    version_info = f"[DEPRECATION{version_display}] "
    if is_atty():
        version_info = f"{Colors.RED}{version_info}"
        message = f"{Colors.YELLOW}{message}{Colors.END}"
    warn(version_info + message, DeprecationWarning)
