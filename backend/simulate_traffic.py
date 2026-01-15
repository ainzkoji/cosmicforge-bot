import time
import sys
import os
import threading

# Add project root to path (backend/)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.runner.runner import PaperRunner
from app.persistence.run_manager import RunManager, Environment
from app.exchange.binance.client import BinanceFuturesClient
from app.core.config import settings

def main():
    print("=" * 50)
    print("🚀 COSMICFORGE TRAFFIC SIMULATOR")
    print("🛡️ MONITORING SYSTEM V2: LOADED")
    print("=" * 50)
    manager = RunManager()
    
    # Start a run to initialize DB state for this session
    try:
        run_id = manager.start_run(
            environment=Environment.PAPER, 
            config={"strategy": "random_forest_v1", "mode": "traffic_simulation"}
        )
    except Exception as e:
         print(f"Start run failed (ignoring for sim): {e}")
         run_id = "sim_run_" + str(int(time.time()))
    
    print(f"Run ID: {run_id}")
    
    # Initialize client (real client needed for runner, even if simulating)
    # The runner uses client for market data (klines, price).
    client = BinanceFuturesClient(
        api_key=settings.BINANCE_API_KEY,
        api_secret=settings.BINANCE_API_SECRET,
        base_url=settings.BINANCE_FAPI_BASE_URL
    )
    
    runner = PaperRunner(client=client)
    runner.run_id = run_id
    runner.cycle_id = 0
    
    # We want to simulate a loop over a few symbols
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    
    print("\nStarting simulated traffic loop.")
    print("This will generate one 'Hold' or 'Signal' trace for each symbol every few seconds.")
    print("Check your dashboard to see them appear live! (Ctrl+C to stop)\n")
    
    try:
        while True:
            runner.cycle_id += 1
            for sym in symbols:
                print(f"[{time.strftime('%H:%M:%S')}] Processing {sym}...", end="", flush=True)
                try:
                    # step_symbol returns a dict result
                    res = runner.step_symbol(sym)
                    
                    # Extract signal from result or audit log?
                    # step_symbol result is dict with keys like 'symbol', 'decision', etc.
                    signal = res.get("signal", "HOLD")
                    print(f" Done. Signal: {signal}")
                except Exception as e:
                    print(f" Error: {e}")
                
                # Small sleep between symbols to stagger updates
                time.sleep(1.5)
            
            print("--- Cycle complete. Waiting 5s ---")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nStopping simulator...")

if __name__ == "__main__":
    main()
