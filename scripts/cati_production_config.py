"""Read-only resolved settings audit; no app startup or database constructor.

Run from any directory. Each backend is imported in its own interpreter so
identically named app packages cannot contaminate one another's configuration.
"""
from __future__ import annotations
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def audit():
    result = {}
    for backend in ("bot", "user", "admin"):
        path = ROOT / "backends" / (backend + "-backend")
        code = """import json,sys
sys.path[:0]=[sys.argv[1],sys.argv[2]]
from app.core.config import settings
template=settings.__class__(_env_file=__import__('pathlib').Path(sys.argv[1])/'.env.example')
assert template.configuration_matrix()==settings.configuration_matrix(), 'LOCAL_ENV_AND_TEMPLATE_DISAGREE'
print(json.dumps(settings.configuration_matrix(),sort_keys=True))
"""
        output = subprocess.check_output([sys.executable, "-c", code, str(path),
                                          str(ROOT / "backends/shared")], cwd=path, text=True)
        result[backend] = json.loads(output)
    if not all(r == result["bot"] for r in result.values()):
        raise ValueError("BACKEND_PRODUCTION_PROFILES_DISAGREE")
    return {"configuration": result["bot"], "backend_profiles_agree": True, "templates_agree": True,
            "scope": "SAVED_CONFIGURATION_ONLY; running services unchanged"}


if __name__ == "__main__":
    print(json.dumps(audit(), indent=2))
