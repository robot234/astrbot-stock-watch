"""First-install only. Stream over trusted SSH after verifying the local package."""
import hashlib
import io
import json
import os
from pathlib import Path
import pwd
import socket
import subprocess
import sys
import tarfile


def command(args):
    return subprocess.run(args, capture_output=True, text=True, timeout=30, check=True).stdout


def main():
    archive, expected, release = sys.argv[1:]
    assert os.geteuid() == 0 and release.replace("-", "").isalnum()
    content = Path(archive).read_bytes()
    assert hashlib.sha256(content).hexdigest() == expected, "archive_hash_mismatch"
    with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as tar:
        members = tar.getmembers()
        assert all(m.isfile() and not m.name.startswith("/") and ".." not in Path(m.name).parts for m in members)
        payload = {m.name: tar.extractfile(m).read() for m in members}
        assert len(payload) == len(members), "duplicate_members"
    manifest = json.loads(payload.pop("manifest.json"))
    assert manifest["release"] == release and set(manifest["files"]) == set(payload)
    assert all(hashlib.sha256(data).hexdigest() == manifest["files"][name] for name, data in payload.items())
    service = payload.get("webapp/deploy/stock-watch-web.service", b"")
    assert b"--artifact /var/lib/stock-watch-web-snapshot/intraday_quotes.json" in service, "artifact_wiring_missing"
    web = Path("/home/pi/apps/stock-watch-web")
    helper = Path("/opt/stock-watch-web-snapshot")
    state = Path("/var/lib/stock-watch-web-snapshot")
    user_unit = Path("/home/pi/.config/systemd/user/stock-watch-web.service")
    system_units = [Path("/etc/systemd/system") / name for name in (
        "stock-watch-web-snapshot.service", "stock-watch-web-snapshot.timer")]
    targets = [web, helper, state, user_unit, *system_units]
    assert all(not p.exists() and not p.is_symlink() for p in targets), "target_exists"
    with socket.socket() as probe:
        probe.bind(("192.168.124.6", 8767))
    user = pwd.getpwnam("pi")
    container = json.loads(command(["docker", "inspect", "--format", "{{json .State}}", "astrbot"]))
    baseline = {"running": container["Running"], "pid": container["Pid"], "started_at": container["StartedAt"],
                "restart_count": int(command(["docker", "inspect", "--format", "{{.RestartCount}}", "astrbot"]))}
    assert baseline["running"] and baseline["pid"] == 296721 and baseline["restart_count"] == 0, "astrbot_baseline_changed"
    plugins = Path("/home/pi/astrbot/data/plugins/astrbot_stock_watch")
    plugin_hashes = {name: hashlib.sha256((plugins / name).read_bytes()).hexdigest()
                     for name in ("main.py", "storage.py", "core.py", "_conf_schema.json")}
    state.mkdir(mode=0o750)
    os.chown(state, 0, user.pw_gid)
    record = {"release": release, "archive_sha256": expected, "initially_absent": [str(p) for p in targets],
              "astrbot": baseline, "plugin_hashes": plugin_hashes}
    (state / "initial-state.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
    web_release = web / "releases" / release
    helper_release = helper / "releases" / release
    web_release.mkdir(parents=True, mode=0o755)
    helper_release.mkdir(parents=True, mode=0o755)
    installed = {}
    for name, data in payload.items():
        if name == "webapp/deploy/snapshot.py":
            destination = helper_release / "snapshot.py"
        elif name == "webapp/deploy/stock-watch-web.service":
            destination = user_unit
        elif name.startswith("webapp/deploy/"):
            destination = Path("/etc/systemd/system") / Path(name).name
        else:
            destination = web_release / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("xb") as handle:
            handle.write(data)
        destination.chmod(0o644)
        installed[str(destination)] = manifest["files"][name]
    (web_release / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    for item in [web, *web.rglob("*"), user_unit]:
        os.chown(item, user.pw_uid, user.pw_gid)
    os.symlink(web_release, web / "current")
    os.lchown(web / "current", user.pw_uid, user.pw_gid)
    os.symlink(helper_release, helper / "current")
    record["installed_files"] = installed
    (state / "initial-state.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == digest for p, digest in installed.items())
    command(["systemctl", "daemon-reload"])
    command(["systemd-analyze", "verify", *map(str, system_units)])
    print(json.dumps({"status": "staged_verified", "release": release, "file_hashes_verified": len(installed),
                      "baseline": baseline, "record": str(state / "initial-state.json")}))


if __name__ == "__main__":
    main()
