"""Restore first-install absence by archiving only this deployment's exact paths."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    release = sys.argv[1]
    check_only = sys.argv[2:] == ["--check"]
    assert os.geteuid() == 0 and release.replace("-", "").isalnum()
    state = Path("/var/lib/stock-watch-web-snapshot")
    record = json.loads((state / "initial-state.json").read_text())
    assert record["release"] == release
    web = Path("/home/pi/apps/stock-watch-web")
    helper = Path("/opt/stock-watch-web-snapshot")
    units = [Path("/home/pi/.config/systemd/user/stock-watch-web.service"),
             Path("/etc/systemd/system/stock-watch-web-snapshot.service"),
             Path("/etc/systemd/system/stock-watch-web-snapshot.timer")]
    assert set(record["initially_absent"]) == set(map(str, [web, helper, state, *units]))
    for path, digest in record["installed_files"].items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest, "installed_content_changed"
    for path in (web, helper):
        assert (path / "current").resolve() == path / "releases" / release
    archives = {p: p.with_name(p.name + ".disabled-" + release) for p in (web, helper, state)}
    assert all(not p.exists() and not p.is_symlink() for p in archives.values())
    if check_only:
        print(json.dumps({"rollback": "validated", "release": release, "production_paths_touched": False}))
        return

    def run(args):
        subprocess.run(args, check=True, capture_output=True, text=True, timeout=330)

    user_systemctl = ["runuser", "-u", "pi", "--", "env", "XDG_RUNTIME_DIR=/run/user/1000", "systemctl", "--user"]
    run(["systemctl", "disable", "--now", "stock-watch-web-snapshot.timer"])
    run(["systemctl", "stop", "stock-watch-web-snapshot.service"])
    run([*user_systemctl, "disable", "--now", "stock-watch-web.service"])
    saved = state / "disabled-units"
    saved.mkdir()
    for path in units:
        path.rename(saved / path.name)
    run(["systemctl", "daemon-reload"])
    run([*user_systemctl, "daemon-reload"])
    for original, archive in archives.items():
        original.rename(archive)
    print(json.dumps({"rollback": "archived", "archives": list(map(str, archives.values()))}))


if __name__ == "__main__":
    main()
