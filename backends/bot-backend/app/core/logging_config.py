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
    
    # Also filter stderr to catch remaining traceback output
    class StderrFilter:
        """Filter stderr to suppress shutdown tracebacks."""
        def __init__(self, stream):
            self.stream = stream
            self.suppress = False
            self.buffer = []
            
        def write(self, text):
            # Check if this is start of a shutdown traceback
            if 'KeyboardInterrupt' in text or 'asyncio.exceptions.CancelledError' in text:
                self.suppress = True
                self.buffer = [text]
                return
            
            if self.suppress:
                self.buffer.append(text)
                # Stop suppressing after the traceback ends
                if text.strip() == '' or not text.startswith(' '):
                    # Check if this was actually a shutdown error
                    full_text = ''.join(self.buffer)
                    if 'capture_signals' not in full_text and 'uvicorn' not in full_text:
                        # Not a shutdown error, write it
                        for buffered in self.buffer:
                            self.stream.write(buffered)
                    self.suppress = False
                    self.buffer = []
                return
            
            self.stream.write(text)
            
        def flush(self):
            self.stream.flush()
    
    # Don't actually replace stderr as it can cause issues
    # The logging filter should be sufficient
