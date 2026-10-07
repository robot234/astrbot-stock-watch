"""Update an installed Web release (install.py is first-install only). Run as root on the Pi:

    sudo /usr/bin/python3 -B update.py runtime.tar.gz <sha256> <release>

Stages the release, probes a temporary loopback instance against the live snapshot, switches
`current` atomically with automatic rollback, then switches the snapshot helper and runs one check.
The plugin directory and the AstrBot container are never touched.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request

WEB = Path("/home/pi/apps/stock-watch-web")
HELPER = Path("/opt/stock-watch-web-snapshot")
STATE = Path("/var/lib/stock-watch-web-snapshot")
USER_UNIT = Path("/home/pi/.config/systemd/user/stock-watch-web.service")
USER_SYSTEMCTL = ["runuser", "-u", "pi", "--", "env", "XDG_RUNTIME_DIR=/run/user/1000", "systemctl", "--user"]
ROUTES = ("version", "overview", "candidates", "research_catalog", "health", "settings", "signals", "intraday",
          "revision", "performance?horizon=5")
STAGING_PORT = 18767


def server_arguments(unit_text):
    """--host/--port/--database/--artifact (and --signals when wired) from the installed unit's ExecStart."""
    line = next((item for item in unit_text.splitlines() if item.startswith("ExecStart=")), "")
    parts = shlex.split(line.split("=", 1)[1]) if line else []
    return {part[2:]: parts[index + 1] for index, part in enumerate(parts[:-1])
            if part in ("--host", "--port", "--database", "--artifact", "--signals")}


def failures(report, revision=None):
    found = [route for route, item in report.items()
             if item.get("http") != 200 or item.get("status") not in ("available", "partial")]
    if (report.get("candidates") or {}).get("research_status") in (None, "unavailable"):
        found.append("candidates:research_missing")
    if revision and (report.get("version") or {}).get("revision") != revision:
        found.append("version:revision_mismatch")
    return found


def probe(host, port):
    report = {}
    for route in ROUTES:
        started = time.monotonic()
        request = urllib.request.Request(f"http://{host}:{port}/api/{route}", headers={"Host": f"{host}:{port}"})
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                body = json.loads(response.read().decode("utf-8"))
                item = {"http": response.status}
        except Exception as error:
            report[route] = {"error": type(error).__name__}
            continue
        meta, data = body.get("meta") or {}, body.get("data") or {}
        item.update(status=meta.get("status"), ms=round((time.monotonic() - started) * 1000))
        if route == "version":
            item["revision"] = (data.get("build") or {}).get("revision")
        elif route == "candidates":
            item["research_status"] = (data.get("research") or {}).get("status")
        report[route] = item
    return report


def point(link, target, owner=None):
    temporary = link.with_name(link.name + ".next")
    if temporary.is_symlink() or temporary.exists():
        temporary.unlink()
    os.symlink(target, temporary)
    if owner:
        os.lchown(temporary, owner.pw_uid, owner.pw_gid)
    os.replace(temporary, link)


def command(arguments, timeout=60):
    done = subprocess.run(arguments, capture_output=True, text=True, timeout=timeout)
    if done.returncode:
        raise RuntimeError(f"command_failed:{arguments[0]}:{done.returncode}:{done.stderr[-200:]}")
    return done.stdout.strip()


def load(archive_path, expected, release):
    content = Path(archive_path).read_bytes()
    if hashlib.sha256(content).hexdigest() != expected:
        raise SystemExit("archive_hash_mismatch")
    with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as tar:
        members = tar.getmembers()
        if not all(m.isfile() and not m.name.startswith("/") and ".." not in PurePosixPath(m.name).parts for m in members):
            raise SystemExit("unsafe_archive_member")
        files = {m.name: tar.extractfile(m).read() for m in members}
    manifest = json.loads(files.pop("manifest.json"))
    if manifest.get("release") != release or set(manifest["files"]) != set(files) or any(
            hashlib.sha256(data).hexdigest() != manifest["files"][name] for name, data in files.items()):
        raise SystemExit("manifest_mismatch")
    return files, manifest


def main():
    import pwd

    archive_path, expected, release = sys.argv[1:4]
    if os.geteuid() != 0 or not release.replace("-", "").isalnum():
        raise SystemExit("run as root with an alphanumeric release id")
    files, manifest = load(archive_path, expected, release)
    revision = json.loads(files.get("webapp/build_info.json", b"{}")).get("revision")
    account = pwd.getpwnam("pi")
    arguments = server_arguments(USER_UNIT.read_text(encoding="utf-8"))
    container = lambda: command(["docker", "inspect", "--format={{.State.Status}} {{.State.StartedAt}} {{.RestartCount}}", "astrbot"])
    helper_idle = lambda: command(["systemctl", "show", "stock-watch-web-snapshot.service", "--property=ActiveState", "--value"]) == "inactive"
    result = {"release": release, "revision": revision, "phase": "preflight", "plugin_touched": False,
              "container_restarted": False, "server": arguments}
    try:
        result["container_before"] = container()
        previous_web = Path(os.path.realpath(WEB / "current"))
        previous_helper = Path(os.path.realpath(HELPER / "current"))
        result.update(previous_web=str(previous_web), previous_helper=str(previous_helper))
        new_web, new_helper = WEB / "releases" / release, HELPER / "releases" / release
        if new_web.exists() or new_helper.exists():
            raise RuntimeError("release_exists")
        if not helper_idle():
            raise RuntimeError("snapshot_service_busy")
        if shutil.disk_usage(WEB).free < 2 * 1024 ** 3:
            raise RuntimeError("disk_space_low")

        result["phase"] = "staging"
        for name, data in files.items():
            if name == "webapp/deploy/snapshot.py":
                destination = new_helper / "snapshot.py"
            elif name.startswith("webapp/deploy/"):
                continue
            else:
                destination = new_web / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("xb") as handle:
                handle.write(data)
            destination.chmod(0o644)
        (new_web / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        for item in [new_web, *new_web.rglob("*")]:
            os.chown(item, account.pw_uid, account.pw_gid)
            if item.is_dir():
                item.chmod(0o755)
        new_helper.chmod(0o755)

        result["phase"] = "staging_probe"
        staging = subprocess.Popen(["runuser", "-u", "pi", "--", "/usr/bin/python3", "-B", "-m", "webapp.server",
                                    "--host", "127.0.0.1", "--port", str(STAGING_PORT), "--database", arguments["database"],
                                    "--artifact", arguments["artifact"],
                                    *(["--signals", arguments["signals"]] if arguments.get("signals") else [])],
                                   cwd=str(new_web), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            time.sleep(3)
            result["staging_probe"] = probe("127.0.0.1", STAGING_PORT)
        finally:
            staging.terminate()
            try:
                staging.wait(10)
            except subprocess.TimeoutExpired:
                staging.kill()
        if failures(result["staging_probe"], revision):
            raise RuntimeError("staging_probe_failed:" + ",".join(failures(result["staging_probe"], revision)))

        result["phase"] = "switch_web"
        point(WEB / "current", new_web, account)
        command(USER_SYSTEMCTL + ["restart", "stock-watch-web.service"])
        report = {}
        for _ in range(15):
            time.sleep(2)
            report = probe(arguments["host"], arguments["port"])
            if not failures(report, revision):
                break
        else:
            point(WEB / "current", previous_web, account)
            command(USER_SYSTEMCTL + ["restart", "stock-watch-web.service"])
            raise RuntimeError("live_probe_failed_rolled_back:" + ",".join(failures(report, revision)))
        result["live_probe"] = report

        result["phase"] = "switch_helper"
        if not helper_idle():
            raise RuntimeError("snapshot_service_busy_after_web_switch")
        point(HELPER / "current", new_helper)
        command(["systemctl", "start", "stock-watch-web-snapshot.service"], timeout=330)
        result["snapshot_status"] = json.loads((STATE / "snapshot_status.json").read_text(encoding="utf-8"))

        record = WEB / "deployments" / release
        record.mkdir(parents=True, exist_ok=False)
        (record / "rollback.sh").write_text("\n".join([
            "#!/bin/bash", "set -eu", "export XDG_RUNTIME_DIR=/run/user/1000",
            f"ln -sfn {previous_web} {WEB}/current.next && mv -Tf {WEB}/current.next {WEB}/current",
            "systemctl --user restart stock-watch-web.service",
            f"sudo -n ln -sfn {previous_helper} {HELPER}/current.next && sudo -n mv -Tf {HELPER}/current.next {HELPER}/current",
            ""]), encoding="utf-8")
        (record / "rollback.sh").chmod(0o755)
        result["rollback_command"] = f"bash {record}/rollback.sh"
        result["container_after"] = container()
        if result["container_after"] != result["container_before"]:
            raise RuntimeError("container_state_changed")
        result["phase"] = "deployed_verified"
        (record / "result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        for item in [record.parent, record, *record.rglob("*")]:
            os.chown(item, account.pw_uid, account.pw_gid)
    except Exception as error:
        result.update(error_type=type(error).__name__, reason=str(error)[:600])
    print(json.dumps(result, ensure_ascii=True))
    return 0 if result["phase"] == "deployed_verified" and "error_type" not in result else 1


if __name__ == "__main__":
    raise SystemExit(main())
