from __future__ import annotations
import json, re, subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
def run(*args: str) -> str:
    return subprocess.check_output(args, cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()

def main() -> int:
    if run("git", "status", "--short"):
        print("FAIL dirty worktree"); return 1
    json.loads((ROOT / "_conf_schema.json").read_text(encoding="utf-8"))
    forbidden = re.compile(r"(password|secret|api[_-]?key)\s*[:=]\s*['\"][^'\"]+", re.I)
    for path in ROOT.glob("*.py"):
        if forbidden.search(path.read_text(encoding="utf-8")):
            print("FAIL possible secret", path); return 1
    print("PASS release checks", run("git", "rev-parse", "HEAD"))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
