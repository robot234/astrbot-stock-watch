"""Daily display-only research signals: overheat label (scheme F), index trend (scheme D), and the
lockup-expiry and margin-crowding risk reminders.

They never change formal gates, candidates or recommendations. Run once each evening with an
interpreter that has numpy, pandas and akshare (the data-probe venv on the Pi):

    python -B -m webapp.research_signals --database SNAPSHOT.sqlite3 --output STATE/research_signals.json

The latest result is replaced atomically. Each new trade date is also appended to the forward
records next to it (research_overheat_forward.jsonl, research_index_trend_forward.jsonl,
research_unlock_forward.jsonl, research_margin_forward.jsonl), which are only ever appended to.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import sys

import numpy as np
import pandas as pd

from .data import Snapshot

SCHEMA = "stock_watch_research_signals/v1"
BOARDS = ("600", "601", "603", "605", "000", "001", "002", "003", "300", "301")
COOL8 = ("R20", "C_MA20", "IVOL20", "VOLR5_60", "MAX20", "TURN5_20", "ABTURN", "RSI14")
PRICE_MAX = 50.0
AMOUNT_MIN = 20_000_000
HISTORY = 60
QUANTILE = 0.9
MIN_NAMES = 30
INDICES = (("000905", "中证500"), ("000852", "中证1000"))
ST_NAME = re.compile(r"^\*?S?\*?ST")
EXCLUSION_ORDER = ("not_main_or_chinext", "st_name", "no_bar_today", "history_lt_60", "corporate_action_60d",
                   "price_above_50", "amount_below_20m", "indicator_missing")
APPROXIMATIONS = (
    "turnover_ratio_from_volume",   # TURN5_20 / ABTURN use volume ratios; equal to turnover ratios while float shares are unchanged
    "st_from_current_name",         # ST taken from the current stock_symbols name, not rebuilt day by day
)
EVIDENCE = "docs/research/NEW_SCHEMES_RESULTS_20261007.md"
CHINA = timezone(timedelta(hours=8))
# Risk reminders rest on published A-share studies only; both thresholds are display choices, not validated here.
UNLOCK_DAYS, UNLOCK_HEAVY = 30, 0.05
MARGIN_LOOKBACK, MARGIN_QUANTILE = 5, 0.9
MARGIN_COLUMNS = {"sse": "标的证券代码", "szse": "证券代码"}


def load_window(database, now):
    """Active raw bars of the published daily generation, pivoted to (trade dates x codes)."""
    db = sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        snapshot = Snapshot(db, "", now)
        active = snapshot.active_raw()
        if not active:
            raise RuntimeError("active_raw_unavailable")
        days = [r[0] for r in db.execute("SELECT trade_date FROM batch_days WHERE batch_id=? ORDER BY trade_date",
                                         (active["batch_id"],))]
        rows = db.execute(
            "SELECT pb.trade_date,pb.code,pb.open,pb.high,pb.low,pb.close,pb.pre_close,pb.volume,pb.amount"
            " FROM partition_bars pb JOIN batch_days bd ON bd.batch_id=? AND bd.trade_date=pb.trade_date"
            " AND bd.partition_id=pb.partition_id", (active["batch_id"],)).fetchall()
        names = {}
        if "stock_symbols" in snapshot.tables:
            names = {str(r["code"]): str(r["name"] or "") for r in db.execute("SELECT code,name FROM stock_symbols")}
        metadata = snapshot.snapshot_metadata()
    finally:
        db.close()
    frame = pd.DataFrame([tuple(r) for r in rows], columns=["trade_date", "code", "open", "high", "low", "close",
                                                             "pre_close", "volume", "amount"])
    codes = sorted(frame["code"].astype(str).unique().tolist())
    market = {}
    for field in ("open", "high", "low", "close", "pre_close", "volume", "amount"):
        market[field] = (frame.pivot(index="trade_date", columns="code", values=field)
                         .reindex(index=days, columns=codes).to_numpy(dtype=np.float64))
    inputs = {"batch_id": active["batch_id"], "trade_date": active["trade_date"], "generation": active["generation"],
              "sessions": len(days), "first_session": days[0] if days else None, "codes": len(codes),
              "snapshot_revision": metadata.get("revision"), "snapshot_captured_at": metadata.get("captured_at")}
    return inputs, days, market, codes, names


def sliding_max_mean(ret, window=20, top=3):
    n, m = ret.shape
    out = np.full((n, m), np.nan)
    if n < window:
        return out
    filled = np.where(np.isfinite(ret), ret, -np.inf)
    counts = pd.DataFrame(np.isfinite(ret).astype(np.int16)).rolling(window, min_periods=window).sum().to_numpy()
    view = np.lib.stride_tricks.sliding_window_view(filled, window, axis=0)
    out[window - 1:] = (-np.partition(-view, top - 1, axis=2)[:, :, :top]).mean(axis=2)
    out[~(counts == window)] = np.nan
    return out


def overheat(market, codes, names, days):
    """Scheme F on the latest session: the COOL8 composite's top decile inside the research pool."""
    o, h, l, c = (market[k] for k in ("open", "high", "low", "close"))
    pc, v, a = market["pre_close"], market["volume"], market["amount"]
    n, m = c.shape
    if n < HISTORY:
        return {"status": "unavailable", "reason": "history_lt_60", "trade_date": days[-1] if days else None}
    board = np.array([code.startswith(BOARDS) for code in codes])
    st = np.array([bool(ST_NAME.match(names.get(code, "").strip().upper())) for code in codes])
    with np.errstate(invalid="ignore"):
        present = (np.isfinite(o) & np.isfinite(h) & np.isfinite(l) & np.isfinite(c) & np.isfinite(v) & np.isfinite(a)
                   & (v > 0) & (a > 0) & (l > 0) & (h >= l))
        valid = present & ~st[None, :]
        prev_close = np.vstack([np.full((1, m), np.nan), c[:-1]])
        action = np.isfinite(prev_close) & np.isfinite(pc) & (np.abs(pc - prev_close) > 0.011)
    valid60 = pd.DataFrame(valid.astype(np.int16)).rolling(HISTORY, min_periods=HISTORY).sum().to_numpy() == HISTORY
    no_action60 = pd.DataFrame(action.astype(np.int16)).rolling(HISTORY, min_periods=HISTORY).sum().to_numpy() == 0
    atr14 = (pd.DataFrame(h) - pd.DataFrame(l)).rolling(14, min_periods=14).mean().to_numpy()
    with np.errstate(invalid="ignore"):
        pool = (valid & valid60 & no_action60 & board[None, :] & (c > 0) & (c <= PRICE_MAX)
                & (a >= AMOUNT_MIN) & np.isfinite(atr14))
    close, volume = pd.DataFrame(c), pd.DataFrame(np.where(v > 0, v, np.nan))
    ret = close / close.shift(1) - 1
    resid = ret.sub(ret.where(pd.DataFrame(pool)).mean(axis=1), axis=0)
    ma20 = close.rolling(20, min_periods=20).mean()
    turn20, turn60 = volume.rolling(20, min_periods=20).mean(), volume.rolling(60, min_periods=60).mean()
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
    factors = {
        "R20": close / close.shift(20) - 1,
        "C_MA20": close / ma20 - 1,
        "IVOL20": resid.rolling(20, min_periods=20).std(),
        "VOLR5_60": volume.rolling(5, min_periods=5).mean() / turn60,
        "MAX20": pd.DataFrame(sliding_max_mean(ret.to_numpy())),
        "TURN5_20": volume.rolling(5, min_periods=5).mean() / turn20,
        "ABTURN": turn20 / turn60,
        "RSI14": 100 - 100 / (1 + gain / loss),
    }
    t = n - 1
    latest = {}
    for key, values in factors.items():
        row = np.array(values, dtype=np.float64)[t]
        row[~np.isfinite(row)] = np.nan
        latest[key] = row
    known = pool[t].copy()
    for key in COOL8:
        known &= np.isfinite(latest[key])
    ix = np.flatnonzero(known)
    reasons = {}
    for j, code in enumerate(codes):
        if known[j]:
            continue
        checks = (not board[j], st[j], not present[t, j], not valid60[t, j], not no_action60[t, j],
                  bool(c[t, j] > PRICE_MAX), bool(a[t, j] < AMOUNT_MIN), True)
        reasons[code] = next(reason for reason, hit in zip(EXCLUSION_ORDER, checks) if hit)
    result = {"status": "available", "rule": "F_HOT_Q90", "trade_date": days[t], "pool_size": int(pool[t].sum()),
              "evaluated": int(len(ix)), "quantile": QUANTILE, "factors": list(COOL8),
              "approximations": list(APPROXIMATIONS), "evidence": EVIDENCE,
              "excluded_counts": {k: sum(1 for r in reasons.values() if r == k) for k in EXCLUSION_ORDER}}
    if len(ix) < MIN_NAMES:
        return {**result, "status": "unavailable", "reason": "pool_lt_30", "hot": [], "stocks": {}, "excluded": reasons}
    total = np.zeros(len(ix))
    for key in COOL8:
        total += pd.Series(latest[key][ix]).rank(pct=True).to_numpy()
    total /= len(COOL8)
    threshold = float(np.quantile(total, QUANTILE))
    pct = pd.Series(total).rank(pct=True).to_numpy()
    stocks = {}
    for k, j in enumerate(ix):
        stocks[codes[j]] = {"score": round(float(total[k]), 4), "pct": round(float(pct[k]), 4),
                            "hot": bool(total[k] >= threshold),
                            "indicators": {key: round(float(latest[key][j]), 6) for key in COOL8}}
    hot = sorted(({"code": code, "name": names.get(code) or None, "score": item["score"], "pct": item["pct"]}
                  for code, item in stocks.items() if item["hot"]), key=lambda x: (-x["score"], x["code"]))
    return {**result, "threshold": round(threshold, 4), "hot_count": len(hot), "hot": hot, "stocks": stocks,
            "excluded": reasons}


def default_fetch(code):
    import akshare as ak  # only the evening job needs it; the web server never imports this path
    return ak.stock_zh_index_daily(symbol="sh" + code)


def index_trend(fetch=None):
    """Scheme D display: each index's close against its 200-session mean (120 shown as a diagnostic)."""
    fetch = fetch or default_fetch
    indices, errors = {}, {}
    for code, name in INDICES:
        try:
            frame = fetch(code)
            series = {}
            for row in frame.to_dict(orient="records"):
                value = float(row["close"])
                if np.isfinite(value) and value > 0:
                    series[str(row["date"])[:10]] = value
            dates = sorted(series)
            close = np.array([series[d] for d in dates])
            if len(close) < 200:
                raise ValueError("history_lt_200")
            ma200 = pd.Series(close).rolling(200, min_periods=200).mean().to_numpy()
            ma120 = pd.Series(close).rolling(120, min_periods=120).mean().to_numpy()
            side = close[-1] > ma200[-1]
            streak = 0
            for value, mean in zip(close[::-1], ma200[::-1]):
                if not np.isfinite(mean) or (value > mean) != side:
                    break
                streak += 1
            indices[code] = {"name": name, "date": dates[-1], "close": round(float(close[-1]), 3),
                             "ma200": round(float(ma200[-1]), 3), "above_ma200": bool(side),
                             "distance_ma200": round(float(close[-1] / ma200[-1] - 1), 6), "sessions_on_side": streak,
                             "ma120": round(float(ma120[-1]), 3), "above_ma120": bool(close[-1] > ma120[-1]),
                             "distance_ma120": round(float(close[-1] / ma120[-1] - 1), 6), "rows": int(len(close))}
        except Exception as error:  # one index failing must not hide the other
            errors[code] = f"{type(error).__name__}: {str(error)[:120]}"
    return {"status": "available" if indices else "unavailable", "rule": "D_MA200",
            "source": "akshare.stock_zh_index_daily (sina)", "indices": indices, "errors": errors, "evidence": EVIDENCE}


def default_unlock_fetch(start, end):
    import akshare as ak  # evening job only
    return ak.stock_restricted_release_detail_em(start_date=start, end_date=end)


def default_margin_fetch(exchange, day):
    import akshare as ak  # evening job only
    return (ak.stock_margin_detail_sse if exchange == "sse" else ak.stock_margin_detail_szse)(date=day)


def finite(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def stock_code(value):
    text = str(value if value is not None else "").strip()
    return text.zfill(6) if text.isdigit() and len(text) <= 6 else text


def unlock_schedule(today, fetch=None):
    """Restricted-share releases from today through the next 30 calendar days, summed per code."""
    fetch = fetch or default_unlock_fetch
    end = today + timedelta(days=UNLOCK_DAYS)
    stocks = {}
    for row in fetch(today.strftime("%Y%m%d"), end.strftime("%Y%m%d")).to_dict(orient="records"):
        code, day = stock_code(row.get("股票代码")), str(row.get("解禁时间") or "")[:10]
        if not re.fullmatch(r"\d{6}", code) or not today.isoformat() <= day <= end.isoformat():
            continue
        ratio, value = finite(row.get("占解禁前流通市值比例")), finite(row.get("实际解禁市值"))
        stocks.setdefault(code, []).append({"date": day, "type": str(row.get("限售股类型") or "")[:20],
                                            "ratio": round(ratio, 6) if ratio is not None else None,
                                            "value": round(value) if value is not None else None})
    summary = {}
    for code, events in stocks.items():
        events.sort(key=lambda event: event["date"])
        total = round(sum(event["ratio"] or 0.0 for event in events), 6)
        summary[code] = {"next_date": events[0]["date"], "ratio_total": total, "heavy": total >= UNLOCK_HEAVY,
                         "events": events[:6]}
    heavy = sorted(([code, item["next_date"], item["ratio_total"]] for code, item in summary.items() if item["heavy"]),
                   key=lambda row: (row[1], row[0]))
    return {"status": "available", "rule": "UNLOCK_30D_FLOAT5", "window": [today.isoformat(), end.isoformat()],
            "events": sum(len(events) for events in stocks.values()), "heavy_threshold": UNLOCK_HEAVY,
            "heavy_count": len(heavy), "heavy": heavy, "stocks": summary,
            "source": "akshare.stock_restricted_release_detail_em (eastmoney)"}


def margin_crowding(days, market, codes, fetch=None):
    """Margin buying over that day's turnover, ranked among margin stocks, on the newest day both exchanges published."""
    fetch = fetch or default_margin_fetch
    index = {code: j for j, code in enumerate(codes)}
    attempts, rows, t = {}, {}, None
    for back in range(1, min(MARGIN_LOOKBACK, len(days)) + 1):
        t, rows, counts = len(days) - back, {}, {}
        for exchange, column in MARGIN_COLUMNS.items():
            records = fetch(exchange, days[t].replace("-", "")).to_dict(orient="records")
            counts[exchange] = len(records)
            for row in records:
                j = index.get(stock_code(row.get(column)))
                buy, balance = finite(row.get("融资买入额")), finite(row.get("融资余额"))
                amount = market["amount"][t, j] if j is not None else np.nan
                if buy is not None and balance is not None and np.isfinite(amount) and amount > 0:
                    rows[codes[j]] = (buy, balance, buy / amount)
        attempts[days[t]] = counts
        if all(counts.values()):
            break
    else:
        return {"status": "unavailable", "reason": "margin_unpublished", "attempts": attempts}
    if len(rows) < MIN_NAMES:
        return {"status": "unavailable", "reason": "margin_rows_lt_30", "trade_date": days[t], "attempts": attempts}
    shares = pd.Series({code: share for code, (_buy, _balance, share) in rows.items()})
    pct, threshold = shares.rank(pct=True), float(shares.quantile(MARGIN_QUANTILE))
    stocks = {code: {"buy": round(buy), "balance": round(balance), "buy_share": round(share, 6),
                     "pct": round(float(pct[code]), 4), "crowded": bool(share >= threshold)}
              for code, (buy, balance, share) in rows.items()}
    crowded = sorted(([code, item["buy_share"]] for code, item in stocks.items() if item["crowded"]),
                     key=lambda row: (-row[1], row[0]))
    return {"status": "available", "rule": "MARGIN_BUY_SHARE_Q90", "trade_date": days[t], "eligible": len(stocks),
            "quantile": MARGIN_QUANTILE, "threshold": round(threshold, 6), "crowded_count": len(crowded),
            "crowded": crowded, "stocks": stocks, "attempts": attempts,
            "source": "akshare.stock_margin_detail_sse / stock_margin_detail_szse (exchanges)"}


def guarded(section):
    try:
        return section()
    except Exception as error:  # a reminder source failing must not hide the others
        return {"status": "unavailable", "reason": f"{type(error).__name__}: {str(error)[:160]}"}


def write_atomic(path, payload):
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")),
                         encoding="utf-8")
    os.replace(temporary, path)


def append_forward(path, key, record):
    """Append once per key; earlier lines are never rewritten."""
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                if json.loads(line).get(key) == record[key]:
                    return False
            except ValueError:
                continue
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n")
    return True


def build(database, output, *, now=None, fetch=None, skip_index=False, unlock_fetch=None, margin_fetch=None):
    """`skip_index` is the offline mode: no index, lockup or margin requests."""
    now = now or datetime.now(timezone.utc)
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema": SCHEMA, "generated_at": now.isoformat(), "display_only": True,
               "formal_tables_written": False, "inputs": None}
    window = None
    try:
        window = load_window(database, now)
        inputs, days, market, codes, names = window
        payload["inputs"] = inputs
        payload["overheat"] = overheat(market, codes, names, days)
    except Exception as error:
        payload["overheat"] = {"status": "unavailable", "reason": f"{type(error).__name__}: {str(error)[:160]}"}
    skipped = {"status": "skipped"}
    payload["index_trend"] = ({"status": "skipped", "indices": {}, "errors": {}} if skip_index else index_trend(fetch))
    payload["unlock"] = skipped if skip_index else guarded(lambda: unlock_schedule(now.astimezone(CHINA).date(), unlock_fetch))
    payload["margin"] = (skipped if skip_index else
                         guarded(lambda: margin_crowding(window[1], window[2], window[3], margin_fetch)) if window else
                         {"status": "unavailable", "reason": "snapshot_unavailable"})
    write_atomic(output, payload)
    recorded = {"overheat": False, "index_trend": False, "unlock": False, "margin": False}
    hot = payload["overheat"]
    if hot.get("status") == "available":
        recorded["overheat"] = append_forward(output.with_name("research_overheat_forward.jsonl"), "trade_date", {
            "trade_date": hot["trade_date"], "recorded_at": payload["generated_at"],
            "batch_id": (payload["inputs"] or {}).get("batch_id"), "generation": (payload["inputs"] or {}).get("generation"),
            "pool_size": hot["pool_size"], "evaluated": hot["evaluated"], "threshold": hot["threshold"],
            "hot": [[item["code"], item["score"]] for item in hot["hot"]]})
    trend = payload["index_trend"]
    if trend.get("indices"):
        key = "|".join(f"{code}:{item['date']}" for code, item in sorted(trend["indices"].items()))
        recorded["index_trend"] = append_forward(output.with_name("research_index_trend_forward.jsonl"), "key", {
            "key": key, "recorded_at": payload["generated_at"],
            "indices": {code: {k: item[k] for k in ("date", "close", "ma200", "above_ma200", "ma120", "above_ma120")}
                        for code, item in trend["indices"].items()}})
    unlock, margin = payload["unlock"], payload["margin"]
    if unlock.get("status") == "available" and payload["inputs"]:
        recorded["unlock"] = append_forward(output.with_name("research_unlock_forward.jsonl"), "trade_date", {
            "trade_date": payload["inputs"]["trade_date"], "recorded_at": payload["generated_at"], "window": unlock["window"],
            "events": unlock["events"], "heavy": unlock["heavy"]})
    if margin.get("status") == "available":
        recorded["margin"] = append_forward(output.with_name("research_margin_forward.jsonl"), "trade_date", {
            "trade_date": margin["trade_date"], "recorded_at": payload["generated_at"], "eligible": margin["eligible"],
            "threshold": margin["threshold"], "crowded": margin["crowded"]})
    payload["forward_recorded"] = recorded
    return payload


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--skip-index", action="store_true", help="Offline run without the index request")
    args = parser.parse_args(argv)
    payload = build(args.database, args.output, skip_index=args.skip_index)
    hot, trend = payload["overheat"], payload["index_trend"]
    print(json.dumps({"overheat": {k: hot.get(k) for k in ("status", "reason", "trade_date", "pool_size", "hot_count")},
                      "index_trend": {"status": trend.get("status"), "errors": trend.get("errors"),
                                      "dates": {c: i.get("date") for c, i in (trend.get("indices") or {}).items()}},
                      "unlock": {k: payload["unlock"].get(k) for k in ("status", "reason", "window", "events", "heavy_count")},
                      "margin": {k: payload["margin"].get(k) for k in ("status", "reason", "trade_date", "eligible", "crowded_count")},
                      "forward_recorded": payload["forward_recorded"]}, ensure_ascii=False), flush=True)
    return 0 if hot.get("status") == "available" and trend.get("status") in ("available", "skipped") else 1


if __name__ == "__main__":
    sys.exit(main())
