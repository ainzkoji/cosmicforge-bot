"""
Global Exception Visibility Module

Provides global exception handlers for:
- Main thread uncaught exceptions
- Asyncio event loop exceptions
- Background task exceptions

Import this module early in main.py to install handlers.
"""

import sys
import traceback
import asyncio


def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
    """Global handler for uncaught exceptions in main thread."""
    print("\n" + "="*80, file=sys.stderr)
    print("🚨 UNCAUGHT EXCEPTION (MAIN THREAD)", file=sys.stderr)
    print("="*80, file=sys.stderr)
    print(f"Exception Type: {exc_type.__name__}", file=sys.stderr)
    print(f"Exception Value: {exc_value}", file=sys.stderr)
    print("\nFull Stack Trace:", file=sys.stderr)
    traceback.print_exception(exc_type, exc_value, exc_traceback, file=sys.stderr)
    print("="*80 + "\n", file=sys.stderr)


def handle_async_exception(loop, context):
    """Global handler for uncaught exceptions in asyncio tasks."""
    print("\n" + "="*80, file=sys.stderr)
    print("🚨 ASYNC EXCEPTION (EVENT LOOP)", file=sys.stderr)
    print("="*80, file=sys.stderr)
    
    exception = context.get("exception")
    if exception:
        print(f"Exception Type: {type(exception).__name__}", file=sys.stderr)
        print(f"Exception Value: {exception}", file=sys.stderr)
        print("\nFull Stack Trace:", file=sys.stderr)
        traceback.print_exception(type(exception), exception, exception.__traceback__, file=sys.stderr)
    else:
        print(f"Message: {context.get('message', 'Unknown async error')}", file=sys.stderr)
        print(f"Context: {context}", file=sys.stderr)
    
    print("="*80 + "\n", file=sys.stderr)


def install_global_exception_handlers():
    """Install all global exception handlers."""
    # Install main thread exception hook
    sys.excepthook = handle_uncaught_exception
    
    # Install asyncio exception handler
    try:
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(handle_async_exception)
    except RuntimeError:
        # Event loop not yet created, will set handler on startup
        pass
    
    print("[STARTUP] ✅ Global exception handlers installed", file=sys.stderr)


# Auto-install on import
install_global_exception_handlers()
