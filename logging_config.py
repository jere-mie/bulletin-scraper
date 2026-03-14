"""
Pretty logging configuration with colors and formatting.
"""

import logging
import sys
from datetime import datetime


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors and nice alignment"""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    # Icons for different levels
    ICONS = {
        'DEBUG': '🔍',
        'INFO': '✓',
        'WARNING': '⚠',
        'ERROR': '✗',
        'CRITICAL': '🔴',
    }
    
    def format(self, record):
        """Format log record with colors and alignment"""
        
        # Color the level name
        level_name = record.levelname
        color = self.COLORS.get(level_name, '')
        icon = self.ICONS.get(level_name, '•')
        
        # Format timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime('%H:%M:%S')
        
        # Get module name (pad to 18 chars for alignment)
        module_name = record.name.split('.')[-1]  # Get last part (e.g., 'scraping' from 'utils.scraping')
        module_name = f"{module_name:<18}"
        
        # Format level with padding
        level_formatted = f"{icon} {level_name:<8}"
        
        # Build the message
        msg = record.getMessage()
        
        # Create the formatted string
        formatted = (
            f"{color}"
            f"[{timestamp}] "
            f"{module_name} "
            f"{level_formatted}"
            f"{self.RESET} "
            f"{msg}"
        )
        
        return formatted


def setup_logging(level=logging.INFO):
    """
    Configure logging with colors and nice formatting.
    
    Args:
        level: Logging level (logging.DEBUG, logging.INFO, etc.)
    
    Returns:
        Logger instance
    """
    # Remove any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Apply custom formatter
    formatter = ColoredFormatter()
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    
    # Suppress verbose third-party loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)


# Print a test message to show the format
if __name__ == '__main__':
    logger = setup_logging(logging.DEBUG)
    logger.debug('This is a debug message')
    logger.info('This is an info message')
    logger.warning('This is a warning message')
    logger.error('This is an error message')
