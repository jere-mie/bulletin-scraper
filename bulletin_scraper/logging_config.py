from __future__ import annotations

import logging
import sys
from datetime import datetime


class ColoredFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    RESET = "\033[0m"
    ICONS = {
        "DEBUG": "?",
        "INFO": "OK",
        "WARNING": "!!",
        "ERROR": "XX",
        "CRITICAL": "!!",
    }

    def format(self, record: logging.LogRecord) -> str:
        level_name = record.levelname
        color = self.COLORS.get(level_name, "")
        icon = self.ICONS.get(level_name, "--")
        timestamp = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        module_name = f"{record.name.split('.')[-1]:<18}"
        level_formatted = f"{icon} {level_name:<8}"
        return f"{color}[{timestamp}] {module_name} {level_formatted}{self.RESET} {record.getMessage()}"


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(ColoredFormatter())

    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    return logging.getLogger(__name__)