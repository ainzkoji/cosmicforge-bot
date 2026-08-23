"""
Logging configuration to suppress noisy shutdown errors.
"""
import logging
import sys


class ShutdownFilter(logging.Filter):
    """Filter to suppress expected shutdown errors."""
    
    def filter(self, record):
        # Suppress KeyboardInterrupt and CancelledError during shutdown
        if record.exc_info:
            exc_type = record.exc_info[0]
            if exc_type in (KeyboardInterrupt, SystemExit):
                return False
            # Suppress asyncio.CancelledError
            if exc_type and exc_type.__name__ == 'CancelledError':
                return False
        
        # Suppress specific shutdown-related messages
        message = record.getMessage()
        shutdown_keywords = [
            'asyncio.exceptions.CancelledError',
            'KeyboardInterrupt',
            'captured_signal',
            'raise_signal'
        ]
        
        for keyword in shutdown_keywords:
            if keyword in message:
                return False
        
        return True


def configure_shutdown_logging():
    """Configure logging to suppress noisy shutdown errors."""
    # Add filter to uvicorn error logger
    uvicorn_error_logger = logging.getLogger("uvicorn.error")
    uvicorn_error_logger.addFilter(ShutdownFilter())
    
    # Add filter to root logger
    root_logger = logging.getLogger()
    root_logger.addFilter(ShutdownFilter())
