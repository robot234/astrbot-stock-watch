"""Build an allowlisted runtime archive and SHA-256 manifest, not a plugin release."""
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile


ROOT = Path(__file__).resolve().parents[2]
FILES = (
    "webapp/__init__.py", "webapp/server.py", "webapp/data.py", "webapp/research_signals.py",
    "data_evidence.py", "paper_forward.py", "paper_review.py", "_conf_schema.json",
    "docs/research/ULTRASHORT_REVERSAL_V1_FROZEN.json", "docs/research/LLM_SECTOR_FIRST_EXP_V0_FROZEN.json",
    "webapp/deploy/snapshot.py", "webapp/deploy/stock-watch-web.service",
    "webapp/deploy/stock-watch-web-snapshot.service",
    "webapp/deploy/stock-watch-web-snapshot.timer",
    "webapp/deploy/stock-watch-research-signals.service", "webapp/deploy/stock-watch-research-signals.timer",
)


def collect(revision=""):
    """With a revision, package the committed bytes so a CRLF checkout cannot change file hashes."""
    if revision:
        git = lambda *args: subprocess.check_output(["git", *args], cwd=ROOT)
        static = git("ls-tree", "-r", "--name-only", revision, "--", "webapp/static").decode().split()
        return {name: git("cat-file", "blob", f"{revision}:{name}") for name in (*FILES, *sorted(static))}
    paths = [ROOT / name for name in FILES]
    paths += sorted(p for p in (ROOT / "webapp/static").rglob("*") if p.is_file())
    return {p.relative_to(ROOT).as_posix(): p.read_bytes() for p in paths}


def main():
    release = sys.argv[1]
    if not release.replace("-", "").isalnum():
        raise ValueError("invalid_release_id")
    revision = sys.argv[2] if len(sys.argv) > 2 else ""
    if revision and not revision.replace("-", "").isalnum():
        raise ValueError("invalid_revision")
    output = ROOT / ".local_records" / "pi-web-deployment" / (release + revision)
    output.mkdir(parents=True, exist_ok=False)
    payload = collect(revision)
    plugin_main = subprocess.check_output(["git", "cat-file", "blob", f"{revision}:main.py"], cwd=ROOT) if revision else None
    payload["webapp/build_info.json"] = json.dumps({
        "release": release, "revision": revision or None,
        "plugin_main_sha256": hashlib.sha256(plugin_main).hexdigest() if plugin_main is not None else None,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}, sort_keys=True).encode()
    manifest = {"release": release, "files": {
        name: hashlib.sha256(data).hexdigest() for name, data in payload.items()}}
    encoded = json.dumps(manifest, indent=2, sort_keys=True).encode()
    (output / "manifest.json").write_bytes(encoded)
    payload["manifest.json"] = encoded
    archive = output / "runtime.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for name, data in payload.items():
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), 0o644
            tar.addfile(info, io.BytesIO(data))
    result = {"archive": str(archive), "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
              "release": release, "files": len(manifest["files"])}
    (output / "package.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
