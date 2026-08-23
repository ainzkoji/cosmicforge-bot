import os
import shutil
import glob

src = r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\user-backend\tests"
dst = r"c:\Users\favou\OneDrive\Desktop\cosmicforge-bot\backends\bot-backend\tests"

files = glob.glob(os.path.join(src, "test_*.py"))
for f in files:
    print(f"Moving {f}")
    shutil.move(f, dst)
print("Done.")
