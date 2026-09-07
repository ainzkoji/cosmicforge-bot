import sys
sys.path.insert(0, '.')
from app.ml.scorer import MLEntryScorer, ACTION_BLOCK, ACTION_SHADOW, ACTION_ALLOW, ACTION_SKIP

s = MLEntryScorer(score_threshold=0.30, shadow_mode=True, enabled=False, hard_block_floor=0.10)

tests = [
    (None,  ACTION_SKIP,   "None score"),
    (0.05,  ACTION_BLOCK,  "score < floor: hard-block even in shadow mode"),
    (0.09,  ACTION_BLOCK,  "score at floor boundary: hard-block"),
    (0.10,  ACTION_SHADOW, "score == floor: above floor, shadow band"),
    (0.20,  ACTION_SHADOW, "score in shadow band"),
    (0.29,  ACTION_SHADOW, "score just below threshold"),
    (0.30,  ACTION_ALLOW,  "score at threshold"),
    (0.75,  ACTION_ALLOW,  "high score"),
]

all_pass = True
for score, expected, label in tests:
    result = s.get_action(score)
    ok = result == expected
    all_pass = all_pass and ok
    status = "OK" if ok else "FAIL"
    print(f"  [{status}] {label}: score={score} -> {result} (expected {expected})")

print()
print("Floor in status():", s.status().get("hard_block_floor"))
print("RESULT:", "ALL PASS" if all_pass else "FAILURES DETECTED")
