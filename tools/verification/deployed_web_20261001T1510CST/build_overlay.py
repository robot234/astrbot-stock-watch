import hashlib, io, json, sys, tarfile
from pathlib import Path
worktree, stage = Path(sys.argv[1]), Path(sys.argv[2])
payload = {"webapp/data.py": (stage / "data.py").read_bytes()}
for p in sorted((worktree / "webapp/static").rglob("*")):
    if p.is_file() and "__pycache__" not in p.parts:
        payload[p.relative_to(worktree).as_posix()] = p.read_bytes()
out = stage / "overlay.tar.gz"
with tarfile.open(out, "w:gz") as tar:
    for name, data in payload.items():
        info = tarfile.TarInfo(name); info.size, info.mode = len(data), 0o644
        tar.addfile(info, io.BytesIO(data))
files = {n: hashlib.sha256(d).hexdigest() for n, d in payload.items()}
(stage / "overlay_files.json").write_text(json.dumps(files, indent=2, sort_keys=True), encoding="utf-8")
print(json.dumps({"archive": str(out), "sha256": hashlib.sha256(out.read_bytes()).hexdigest(), "files": len(files)}))
