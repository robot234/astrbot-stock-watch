"""The Web runtime package must carry committed bytes, independent of the checkout's line endings."""
from pathlib import Path
import subprocess

import pytest

from webapp.deploy.package import FILES, collect


ROOT = Path(__file__).resolve().parents[2]


def _git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)


def test_revision_package_matches_committed_blobs():
    try:
        head = _git("rev-parse", "HEAD").decode().strip()
    except (OSError, subprocess.CalledProcessError):
        pytest.skip("not a git checkout")
    payload = collect(head)
    assert set(FILES).issubset(payload)
    assert {"webapp/static/app.js", "webapp/static/index.html", "webapp/static/icons/LICENSE"}.issubset(payload)
    for name, data in payload.items():
        assert data == _git("cat-file", "blob", f"{head}:{name}"), name
    assert not any(name.startswith(("tests/", ".local_records/")) or name.endswith(".sqlite3") for name in payload)
