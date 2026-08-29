from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
TEXT_SUFFIXES = (".py", ".json", ".yaml", ".yml", ".md")
FORBIDDEN_PATH_PARTS = (
    "tests/",
    "__pycache__/",
    ".pytest_cache/",
    ".env",
    ".sqlite",
    ".db",
    "logs/",
)
SECRET_RE = re.compile(
    r"\b(?:password|secret|api[_-]?key|tushare[_-]?token)\b"
    r"\s*(?:[:=]|\"\s*:)\s*[\"']\s*[^\"'\s][^\"']*[\"']",
    re.IGNORECASE,
)


def run(*args: str) -> str:
    return subprocess.check_output(args, cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()


def _version_from_files() -> tuple[str, str, str] | None:
    metadata = (ROOT / "metadata.yaml").read_text(encoding="utf-8")
    metadata_match = re.search(r"^version:\s*([^\s#]+)", metadata, re.MULTILINE)
    main = (ROOT / "main.py").read_text(encoding="utf-8")
    register_versions = re.findall(r"@register\([^)]*[\"']([^\"']+)[\"']\s*\)", main, re.DOTALL)
    if not metadata_match or len(register_versions) != 1:
        return None
    return metadata_match.group(1), register_versions[0], register_versions[0]


def _check_schema(schema_path: Path) -> str | None:
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return f"invalid config JSON: {exc}"
    if not isinstance(schema, dict) or not schema:
        return "config schema must be a non-empty object"
    allowed_types = {"bool", "int", "float", "string"}
    for key, item in schema.items():
        if not isinstance(key, str) or not isinstance(item, dict):
            return f"invalid config entry: {key!r}"
        if item.get("type") not in allowed_types or "default" not in item or not isinstance(item.get("description"), str):
            return f"invalid config entry: {key}"
    return None


def main() -> int:
    if run("git", "status", "--short"):
        print("FAIL dirty worktree")
        return 1

    versions = _version_from_files()
    if not versions:
        print("FAIL unable to locate one metadata version and one register version")
        return 1
    metadata_version, register_version, _ = versions
    if not VERSION_RE.fullmatch(metadata_version) or metadata_version != register_version:
        print("FAIL version mismatch", metadata_version, register_version)
        return 1

    schema_error = _check_schema(ROOT / "_conf_schema.json")
    if schema_error:
        print("FAIL", schema_error)
        return 1
    storage = (ROOT / "storage.py").read_text(encoding="utf-8")
    schema_match = re.search(r"^LATEST_SCHEMA_VERSION\s*=\s*(\d+)\s*$", storage, re.MULTILINE)
    if not schema_match or schema_match.group(1) != "13":
        print("FAIL expected LATEST_SCHEMA_VERSION=13")
        return 1

    tracked = [name.replace("\\", "/") for name in run("git", "ls-files").splitlines() if name]
    for name in tracked:
        lowered = name.lower()
        if any(part in lowered for part in FORBIDDEN_PATH_PARTS):
            print("FAIL prohibited release artifact", name)
            return 1
        if lowered.startswith("test_") or "/test_" in lowered:
            print("FAIL test file tracked", name)
            return 1
    for name in tracked:
        path = ROOT / name
        if path.suffix.lower() in TEXT_SUFFIXES and SECRET_RE.search(path.read_text(encoding="utf-8")):
            print("FAIL possible secret", name)
            return 1

    print("PASS release checks", run("git", "rev-parse", "HEAD"), "version", metadata_version, "schema", schema_match.group(1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
