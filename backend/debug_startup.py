import sys
import logging
from app.ops.run_manager import RunManager

# Setup logging
logging.basicConfig(level=logging.INFO)

def test_startup():
    print("--- TESTING RUN MANAGER STARTUP ---")
    try:
        rm = RunManager()
        print("1. Creating RunManager...")
        
        print("2. Calling rm.start()...")
        info = rm.start()
        
        print(f"3. SUCCESS! Run started: {info}")
        
    except Exception as e:
        print("\n!!! CRASH DETECTED !!!\n")
        print(e)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_startup()
