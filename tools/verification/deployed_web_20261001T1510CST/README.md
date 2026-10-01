# Stock Watch Web release ui-redesign-20261001T1510CST

This folder records what the Raspberry Pi Web service actually runs since 2026-10-01 15:10 CST.
It differs from `webapp/data.py` in this worktree, so keep it until the code lines are merged.

- Pi path: `/home/pi/apps/stock-watch-web/releases/ui-redesign-20261001T1510CST`
- Derived from: `schema19-20260923T1115CST` (kept on the Pi for rollback)
- Snapshot database at deploy time: schema 20

Files:

- `deployed_data.py`: the live `webapp/data.py` (sha256 `2581bd4b…`). It is `live_schema19_data.py`
  plus the read-only `attach_latest_closes` method, and the candidates route calls it.
  It does not expose research pools, so the research page shows `research_not_exposed`.
- `live_schema19_data.py`: the previous live `webapp/data.py` (sha256 `a823a3c8…`).
- `overlay_files.json`: sha256 of every file in the overlay; the static files match `webapp/static/` here.
- `build_overlay.py`, `remote_stage.py`, `remote_switch.sh`: the scripts used to build, stage and switch.

Rollback on the Pi:

```bash
ln -sfn /home/pi/apps/stock-watch-web/releases/schema19-20260923T1115CST /home/pi/apps/stock-watch-web/current
XDG_RUNTIME_DIR=/run/user/1000 systemctl --user restart stock-watch-web.service
```
