import hashlib, io, json, shutil, sys, tarfile
from pathlib import Path
archive, expected, release = sys.argv[1:4]
assert release.replace("-", "").isalnum(), "invalid_release"
content = Path(archive).read_bytes()
assert hashlib.sha256(content).hexdigest() == expected, "archive_hash_mismatch"
web = Path("/home/pi/apps/stock-watch-web")
base = (web / "current").resolve()
new = web / "releases" / release
assert base.parent == web / "releases" and not new.exists(), "target_exists_or_bad_base"
with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as tar:
    members = tar.getmembers()
    assert all(m.isfile() and not m.name.startswith("/") and ".." not in Path(m.name).parts for m in members)
    payload = {m.name: tar.extractfile(m).read() for m in members}
assert all(n == "webapp/data.py" or n.startswith("webapp/static/") for n in payload), "unexpected_member"
shutil.copytree(base, new, symlinks=True, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
for name, data in payload.items():
    dest = new / name
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data); dest.chmod(0o644)
manifest = json.loads((new / "manifest.json").read_text())
manifest["derived_from"] = base.name
manifest["release"] = release
for name, data in payload.items():
    manifest["files"][name] = hashlib.sha256(data).hexdigest()
(new / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
checked = 0
for name, digest in manifest["files"].items():
    p = new / name
    if p.exists():
        assert hashlib.sha256(p.read_bytes()).hexdigest() == digest, "hash_mismatch:" + name
        checked += 1
print(json.dumps({"status": "staged", "release": str(new), "derived_from": base.name, "overlay_files": len(payload), "hashes_verified": checked}))
