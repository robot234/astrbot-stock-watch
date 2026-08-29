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
    forbidden = re.compile(r"(password|secret|api[_-]?key|tushare_token)\s*[:=]\s*['\"][^'\"]+", re.I)
    tracked = run("git", "ls-files").splitlines()
    banned = ("tests/", "__pycache__/", ".pytest_cache/", ".env", ".sqlite", ".db", "logs/")
    for name in tracked:
        if any(part in name for part in banned):
            print("FAIL prohibited release artifact", name); return 1
    for path in (ROOT / name for name in tracked if name.endswith((".py", ".json", ".yaml", ".yml", ".md"))):
        if forbidden.search(path.read_text(encoding="utf-8")):
            print("FAIL possible secret", path); return 1
    print("PASS release checks", run("git", "rev-parse", "HEAD"))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
