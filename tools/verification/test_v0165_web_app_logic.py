"""Front-end display logic for the 2026-10-07 audit fixes, run in Node without a browser."""
from pathlib import Path
import shutil
import subprocess

import pytest


HARNESS = Path(__file__).with_name("web_app_logic.cjs")


@pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")
def test_app_display_logic():
    result = subprocess.run(["node", str(HARNESS)], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr or result.stdout
    assert '"status":"passed"' in result.stdout
