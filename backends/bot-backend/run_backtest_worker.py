import sys
import asyncio
import signal
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

import logging
from app.backtest.worker import BacktestWorker
from app.core.logging_config import configure_shutdown_logging
from shared_lib.persistence.db import DB

# Configure logging
logging.basicConfig(level=logging.INFO)
configure_shutdown_logging()
logger = logging.getLogger("backtest_worker")

async def run_worker():
    logger.info("Starting Backtest Worker Service...")
    
    db = DB()
    worker = BacktestWorker(db=db)
    
    # Handle shutdown signals
    stop_event = asyncio.Event()
    
    def handle_signal():
        if not stop_event.is_set():
            logger.info("Shutdown signal received, stopping worker...")
            stop_event.set()
    
    # Register signals (might not work in all Windows environments, but good practice)
    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, handle_signal)
        loop.add_signal_handler(signal.SIGTERM, handle_signal)
    except NotImplementedError:
        # Windows loop might not support add_signal_handler
        pass

    try:
        # Start worker loop
        await worker.start()
        
        # Keep running until stop event
        while not stop_event.is_set():
            await asyncio.sleep(1)
            
    except Exception as e:
        logger.error(f"Worker crashed: {e}", exc_info=True)
    finally:
        worker.stop()
        logger.info("Worker stopped.")

if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
