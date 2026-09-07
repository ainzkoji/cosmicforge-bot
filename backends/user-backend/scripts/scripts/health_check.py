import os
import pkgutil
import importlib
import sys
import traceback

# Add backend to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def check_modules():
    print("🔍 Starting Backend Health Check...")
    print(f"📂 Scanning: {os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'app'))}")
    
    error_count = 0
    checked_count = 0
    
    # Walk through the app directory
    root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    app_path = os.path.join(root_path, 'app')
    
    failed_modules = []
    
    for root, dirs, files in os.walk(app_path):
        if 'tests' in root:
            continue
            
        for file in files:
            if 'test_' in file:
                continue
            if file.endswith('.py') and file != '__init__.py':
                # Construct module path
                rel_path = os.path.relpath(os.path.join(root, file), root_path)
                module_name = rel_path.replace(os.sep, '.').replace('.py', '')
                
                checked_count += 1
                try:
                    importlib.import_module(module_name)
                    # print(f"✅ {module_name}")
                except Exception as e:
                    error_count += 1
                    error_msg = f"❌ {module_name}: {type(e).__name__}: {str(e)}"
                    print(error_msg)
                    failed_modules.append((module_name, str(e)))
                    # traceback.print_exc()

    with open("health_report.txt", "w", encoding="utf-8") as f:
        f.write(f"Verified {checked_count} modules.\n")
        if error_count == 0:
            f.write("✅ ALL MODULES IMPORTED SUCCESSFULLY!\n")
        else:
            f.write(f"🚫 FOUND {error_count} BROKEN MODULES:\n")
            for name, err in failed_modules:
                f.write(f"   - {name}: {err}\n")
    
    print("Report written to health_report.txt")

if __name__ == "__main__":
    check_modules()
