"""Offline paper-entry review; never creates orders or recommendation records.

Inputs must carry a frozen record identity and an independently timed simulated
fill. The legacy recommendation-date outcome API has a different day zero.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import math
from pathlib import Path
import sqlite3

try:
    from . import paper_forward as forward
except ImportError:  # Verification also imports this local-only module directly.
    import paper_forward as forward


HORIZONS = (1, 3, 5)


def _positive(value):
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def review(samples: list[dict], sessions: list[str], *, as_of: str) -> dict:
    """Evaluate explicit simulated fills at later verified trading sessions.

    A bar is a close observation keyed by date. Missing or contradictory data
    stays unknown. No intraday high/low ordering or actual execution is inferred.
    """
    ordered = sorted(set(sessions))
    date.fromisoformat(as_of)
    if ordered != sessions or any(date.fromisoformat(day) > date.fromisoformat(as_of) for day in ordered):
        raise ValueError("sessions_must_be_sorted_completed_as_of")
    results = []
    for sample in samples:
        record_id = str(sample.get("record_id") or "")
        if not record_id:
            raise ValueError("record_id_required")
        state = str(sample.get("state") or "unknown")
        fill = str(sample.get("fill_status") or "unknown")
        entry_date = str(sample.get("entry_date") or "")
        entry_price = _positive(sample.get("entry_price"))
        fee = sample.get("round_trip_fee_pct")
        try:
            fee = float(fee)
        except (TypeError, ValueError, OverflowError):
            fee = math.nan
        try:
            exit_slippage = float(sample.get("exit_slippage_pct", 0.0))
        except (TypeError, ValueError, OverflowError):
            exit_slippage = math.nan
        if not math.isfinite(exit_slippage) or exit_slippage < 0:
            exit_slippage = math.nan
        new_accounting = sample.get("accounting_version") == forward.ACCOUNTING_VERSION
        if state == "not_triggered":
            status = "not_triggered"
        elif state != "confirmed":
            status = "unknown" if state == "unknown" else state
        elif fill == "unfilled":
            status = "unfilled"
        elif fill != "simulated_fill" or entry_date not in ordered or entry_price is None or (
                new_accounting and not sample.get("terms_valid")) or (
                not new_accounting and (not math.isfinite(fee) or fee < 0 or not math.isfinite(exit_slippage))):
            status = "unknown"
        else:
            status = "simulated_fill"
        marks = {}
        observations = {}
        for bar in sample.get("bars", []):
            if isinstance(bar, dict):
                observations.setdefault(str(bar.get("date")), []).append(_positive(bar.get("close")))
        for horizon in HORIZONS:
            item = {"status": status, "mark_date": None, "technical_return_pct": None,
                    "net_return_pct": None, "return_kind": "mark_to_close" if new_accounting else "legacy_percent_fee",
                    "return_pct": None, "reason": None}
            if status == "simulated_fill":
                target_index = ordered.index(entry_date) + horizon
                if target_index >= len(ordered):
                    item.update(status="pending", reason="trading_day_not_mature")
                else:
                    day = ordered[target_index]
                    item["mark_date"] = day
                    values = observations.get(day, [])
                    if not values:
                        item.update(status="unknown", reason="close_observation_missing")
                    elif any(value is None for value in values):
                        item.update(status="unknown", reason="invalid_close_observation")
                    elif any(not math.isclose(value, values[0], rel_tol=1e-10, abs_tol=1e-8) for value in values[1:]):
                        item.update(status="unknown", reason="conflicting_close_observations")
                    elif new_accounting and day not in sample.get("mark_valid_dates", ()):
                        item.update(status="unknown", reason="holding_period_evidence_unverified")
                    elif new_accounting:
                        try:
                            percent = forward.close_mark(values[0], quantity=sample["quantity"],
                                                         total_cost_cny=sample["total_cost_cny"])
                            item.update(status="complete", return_pct=float(percent))
                        except (KeyError, TypeError, ValueError, ArithmeticError):
                            item.update(status="unknown", reason="entry_cost_or_close_unverified")
                    else:
                        price = values[0]
                        gross = (price / entry_price - 1) * 100
                        item.update(status="complete", technical_return_pct=gross,
                                    net_return_pct=gross - fee - exit_slippage)
            marks[horizon] = item
        results.append({"record_id": record_id, "state": state, "fill_status": fill,
                        "current_qualification_state": sample.get("current_qualification_state", "unknown"),
                        "current_qualification_version": sample.get("current_qualification_version"),
                        "entry_qualification_version": sample.get("entry_qualification_version"),
                        "entry_date": entry_date or None, "confirmed_at": sample.get("confirmed_at"),
                        "day_zero": "simulated_entry_date",
                        "entry_price": entry_price if status == "simulated_fill" else None,
                        "accounting_version": sample.get("accounting_version") or "legacy-percent-fee-v1",
                        "protocol_version": sample.get("protocol_version") or "legacy-unversioned",
                        "quantity": sample.get("quantity") if new_accounting and status == "simulated_fill" else None,
                        "entry_fees_cny": sample.get("entry_fees_cny") if new_accounting and status == "simulated_fill" else None,
                        "total_cost_cny": sample.get("total_cost_cny") if new_accounting and status == "simulated_fill" else None,
                        "marks": marks})
    summary = {"recommended": len(results),
               "triggered": sum(row["state"] == "confirmed" for row in results),
               "executable_simulated": sum(row["fill_status"] == "simulated_fill" and row["entry_price"] is not None for row in results),
               "horizons": {}}
    for horizon in HORIZONS:
        legacy_rows = [row for row in results if row["accounting_version"] == "legacy-percent-fee-v1"]
        mark_rows = [row for row in results if row["accounting_version"] == forward.ACCOUNTING_VERSION]
        complete = [row["marks"][horizon] for row in legacy_rows
                    if row["marks"][horizon]["status"] == "complete"]
        marked = [row["marks"][horizon] for row in mark_rows
                  if row["marks"][horizon]["status"] == "complete"]
        legacy_positive = sum(row["net_return_pct"] > 0 for row in complete)
        marked_positive = sum(row["return_pct"] > 0 for row in marked)
        summary["horizons"][horizon] = {
            "mature_evaluable": len(complete), "net_positive": legacy_positive,
            "net_positive_rate_evaluable": legacy_positive / len(complete) if complete else None,
            "all_sample_net_positive_rate": legacy_positive / len(legacy_rows) if legacy_rows else None,
            "unknown_or_pending": sum(row["marks"][horizon]["status"] in {"unknown", "pending"} for row in legacy_rows),
            "mark_to_close": {"mature_evaluable": len(marked), "positive": marked_positive,
                              "positive_rate_evaluable": marked_positive / len(marked) if marked else None,
                              "original_candidate_count": len(mark_rows),
                              "unknown_or_pending": sum(row["marks"][horizon]["status"] in {"unknown", "pending"} for row in mark_rows)},
        }
    return {"basis": "versioned_mark_to_close" if any(
                row["accounting_version"] == forward.ACCOUNTING_VERSION for row in results)
                else "offline_simulated_entry_day_zero", "as_of": as_of,
            "records": results, "summary": summary}


def _entry_chain_valid(db, record_id: str, entry, terms) -> bool:
    """Reopen the persisted source chain, not merely its displayed digest."""
    import json
    try:
        if not forward.validated_terms(entry, terms) or terms["arm"] != "B":
            return False
        bound = db.execute(
            "SELECT q.*,p.close FROM paper_qualification_events q JOIN research_pool_picks p "
            "ON p.run_id=q.run_id AND p.code=q.code AND p.pool='primary' "
            "WHERE q.record_id=? AND q.version=?",
            (record_id, entry["qualification_version"]),
        ).fetchone()
        frozen = db.execute(
            "SELECT * FROM paper_freeze_contracts WHERE run_id=?", (bound["run_id"],)
        ).fetchone() if bound else None
        if not bound or bound["state"] != "eligible" or not frozen or frozen["rules_sha256"] != terms["freeze_rules_sha256"]:
            return False
        flags = json.loads(bound["risk_json"])
        if any(flags.get(field) is not False for field in ("suspended", "limit_up", "limit_down", "st")):
            return False
        frozen_at = datetime.fromisoformat(frozen["frozen_at"])
        frozen_at = frozen_at.replace(tzinfo=timezone.utc) if frozen_at.tzinfo is None else frozen_at.astimezone(timezone.utc)
        first_at = forward.aware(entry["first_bar_at"])
        second_at = forward.aware(entry["second_bar_at"])
        quote_at = forward.aware(terms["quote_at"])
        confirmed = forward.aware(entry["confirmed_at"])
        local_first = first_at.astimezone(forward.CHINA)
        local_second = second_at.astimezone(forward.CHINA)
        local_quote = quote_at.astimezone(forward.CHINA)
        def continuous_session(moment):
            wall = moment.time()
            if datetime.min.time().replace(hour=9, minute=30) <= wall < datetime.min.time().replace(hour=11, minute=30):
                return "AM"
            if datetime.min.time().replace(hour=13) <= wall < datetime.min.time().replace(hour=15):
                return "PM"
            return None
        if (second_at - first_at != timedelta(minutes=1) or not frozen_at < first_at < second_at < quote_at < confirmed
                or not forward.in_entry_window(confirmed, "B") or not forward.in_entry_window(quote_at, "B")
                or local_first.date() != local_second.date() or local_first.date() != local_quote.date()
                or not continuous_session(local_first) or continuous_session(local_first) != continuous_session(local_second)
                or continuous_session(local_first) != continuous_session(local_quote)
                or confirmed - (second_at + timedelta(minutes=1)) > timedelta(seconds=120)
                or confirmed - quote_at > timedelta(seconds=120)
                or forward.aware(bound["first_decided_at"]) >= first_at):
            return False
        reference = Decimal(str(bound["close"]))
        if not reference.is_finite() or reference <= 0:
            return False
        execution = json.loads(terms["execution_evidence_json"])
        quote_price = Decimal(str(execution["quote_price"]))
        filled_price = Decimal(str(entry["entry_price"]))
        b_limit = forward.b_limit_price(reference)
        if (not quote_price.is_finite() or quote_price < reference * Decimal("0.995")
                or quote_price > b_limit or filled_price > b_limit):
            return False
        for start, column in ((first_at, "first_bar_evidence_hash"), (second_at, "second_bar_evidence_hash")):
            row = db.execute(
                "SELECT * FROM paper_bar_observations WHERE record_id=? AND qualification_version=? AND bar_start=?",
                (record_id, entry["qualification_version"], start.isoformat()),
            ).fetchone()
            if not row or row["evidence_sha256"] != terms[column]:
                return False
            evidence = json.loads(row["evidence_json"])
            if not (reference * Decimal("1.005") <= Decimal(str(row["bar_close"])) <= reference * Decimal("1.03")):
                return False
            digest = forward.verify_fixture_evidence(
                evidence, kind="bar", reference_at=start, received_by=quote_at, bar_close=row["bar_close"])
            if digest != row["evidence_sha256"] or forward.aware(evidence["received_at"]) >= quote_at:
                return False
        digest = forward.verify_fixture_evidence(
            execution, kind="execution", reference_at=quote_at, received_by=confirmed,
            record_id=record_id, price=execution["quote_price"])
        return digest == terms["execution_evidence_hash"]
    except (KeyError, TypeError, ValueError, ArithmeticError, json.JSONDecodeError):
        return False


def _forward_closes(db, code: str, entry_date: str, sessions: list[str], cutoff: datetime) -> dict[str, float]:
    """Verify the whole holding interval before accepting any close mark."""
    import hashlib
    import json

    valid = {}
    try:
        date.fromisoformat(entry_date)
    except (TypeError, ValueError):
        return valid
    if entry_date not in sessions:
        return valid
    for horizon in HORIZONS:
        index = sessions.index(entry_date) + horizon
        if index >= len(sessions):
            continue
        target = sessions[index]
        days = []
        cursor = date.fromisoformat(entry_date)
        end = date.fromisoformat(target)
        if end > cutoff.astimezone(forward.CHINA).date():
            continue
        complete_calendar = True
        while cursor <= end:
            day = cursor.isoformat()
            row = db.execute("SELECT is_open,status,fetched_at FROM trading_calendar WHERE trade_date=?", (day,)).fetchone()
            try:
                if not row or row["status"] not in {"open", "closed"} or row["is_open"] != int(row["status"] == "open"):
                    complete_calendar = False
                    break
                fetched = forward.aware(row["fetched_at"])
                if fetched > cutoff:
                    complete_calendar = False
                    break
            except (TypeError, ValueError):
                complete_calendar = False
                break
            if row["is_open"]:
                days.append(day)
            cursor += timedelta(days=1)
        if not complete_calendar or days != sessions[sessions.index(entry_date):index + 1]:
            continue
        evidence_good = True
        for day in days:
            facts = db.execute(
                "SELECT * FROM paper_no_action_evidence WHERE code=? AND trade_date=? AND received_at<=?",
                (code, day, cutoff.isoformat()),
            ).fetchall()
            if len(facts) != 1:
                evidence_good = False
                break
            fact = facts[0]
            payload = {"code": code, "trade_date": day, "status": fact["status"],
                       "observed_at": fact["observed_at"], "received_at": fact["received_at"],
                       "source": fact["source"]}
            digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            try:
                observed = forward.aware(fact["observed_at"])
                received = forward.aware(fact["received_at"])
                close_at = datetime.combine(date.fromisoformat(day), datetime.min.time(), forward.CHINA).replace(hour=15).astimezone(timezone.utc)
                if (fact["status"] != "none" or fact["source"] != "synthetic_fixture"
                        or fact["response_sha256"] != digest or observed < close_at
                        or received < observed or received > cutoff):
                    evidence_good = False
                    break
            except (TypeError, ValueError):
                evidence_good = False
                break
            factor = db.execute(
                "SELECT adj_factor,conflicted FROM corporate_action_factors WHERE code=? AND trade_date=?",
                (code, day),
            ).fetchone()
            if factor and (factor["conflicted"] or factor["adj_factor"] != 1):
                evidence_good = False
                break
        if not evidence_good:
            continue
        bar = db.execute(
            "SELECT close,price_basis,fetched_at,corporate_action_factor,corporate_action_evidence "
            "FROM daily_bars WHERE code=? AND trade_date=?", (code, target)
        ).fetchone()
        if not bar or bar["price_basis"] != "unadjusted":
            continue
        try:
            fetched = forward.aware(bar["fetched_at"])
            close_at = datetime.combine(date.fromisoformat(target), datetime.min.time(), forward.CHINA).replace(hour=15).astimezone(timezone.utc)
            close = float(bar["close"])
            if (fetched < close_at or fetched > cutoff or not math.isfinite(close) or close <= 0
                    or bar["corporate_action_factor"] != 1 or not bar["corporate_action_evidence"]):
                continue
        except (TypeError, ValueError, OverflowError):
            continue
        valid[target] = close
    return valid


def review_persisted(db_path: str | Path, sessions: list[str], *, as_of: str,
                     cutoff_at=None) -> dict:
    """Review the same frozen identities and simulated fills shown by Web.

    Only unadjusted closes with a stable, dated corporate-action factor can
    enter a numeric return. Missing provenance remains an unknown mark.
    """
    db = sqlite3.connect(f"file:{Path(db_path).as_posix()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA query_only=ON")
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        required = {"research_pool_runs", "research_pool_picks", "paper_qualification_events",
                    "paper_simulated_entries", "daily_bars", "corporate_action_factors"}
        if not required.issubset(tables):
            raise ValueError("paper_review_tables_unavailable")
        picks = db.execute(
            "SELECT r.run_id,r.trade_date,p.code FROM research_pool_runs r "
            "JOIN research_pool_picks p ON p.run_id=r.run_id AND p.pool='primary' "
            "WHERE r.trade_date<=? ORDER BY r.trade_date,r.run_id,p.code", (as_of,),
        ).fetchall()
        samples = []
        cutoff = (forward.aware(cutoff_at) if cutoff_at is not None else
                  datetime.combine(date.fromisoformat(as_of), datetime.max.time(),
                                   timezone(timedelta(hours=8))).astimezone(timezone.utc))
        for pick in picks:
            record_id = f"{pick['run_id']}:{pick['code']}"
            qualified = None
            for candidate in db.execute(
                "SELECT * FROM paper_qualification_events WHERE record_id=? ORDER BY first_decided_at DESC",
                (record_id,),
            ):
                try:
                    observed = datetime.fromisoformat(candidate["first_decided_at"])
                    if observed.tzinfo and observed.astimezone(timezone.utc) <= cutoff:
                        qualified = candidate
                        break
                except (TypeError, ValueError):
                    continue
            entry = db.execute("SELECT * FROM paper_simulated_entries WHERE record_id=?", (record_id,)).fetchone()
            if entry:
                try:
                    confirmed = datetime.fromisoformat(entry["confirmed_at"])
                    if confirmed.tzinfo is None or confirmed.astimezone(timezone.utc) > cutoff:
                        entry = None
                except (TypeError, ValueError):
                    entry = None
            sample = {"record_id": record_id, "state": "unknown", "fill_status": "unknown", "bars": [],
                      "current_qualification_state": qualified["state"] if qualified else "unknown",
                      "current_qualification_version": qualified["version"] if qualified else None}
            # Every frozen pick belongs to its rule cohort, including candidates
            # that never qualify or enter.  Entry existence cannot define the
            # original denominator of a forward paper study.
            frozen = db.execute(
                "SELECT protocol_version FROM paper_freeze_contracts WHERE run_id=?",
                (pick["run_id"],),
            ).fetchone() if "paper_freeze_contracts" in tables else None
            if frozen:
                protocol = str(frozen["protocol_version"] or "")
                sample.update(protocol_version=protocol,
                              accounting_version=(forward.ACCOUNTING_VERSION
                                                  if protocol == forward.PROTOCOL_VERSION
                                                  else "unknown-accounting-version"))
            if qualified and qualified["state"] == "eligible":
                sample["state"] = "pending"
            bound = db.execute(
                "SELECT state,first_decided_at FROM paper_qualification_events WHERE record_id=? AND version=?",
                (record_id, entry["qualification_version"]),
            ).fetchone() if entry else None
            try:
                bound_at = datetime.fromisoformat(bound["first_decided_at"]) if bound else None
                confirmed_at = datetime.fromisoformat(entry["confirmed_at"]) if entry else None
                binding_valid = bool(bound and bound["state"] == "eligible" and bound_at and confirmed_at
                                     and bound_at.tzinfo and confirmed_at.tzinfo
                                     and bound_at.astimezone(timezone.utc) <= confirmed_at.astimezone(timezone.utc))
            except (TypeError, ValueError):
                binding_valid = False
            if entry and binding_valid:
                sample.update(accounting_version=entry["accounting_version"],
                              terms_valid=False if entry["accounting_version"] == forward.ACCOUNTING_VERSION else None)
                sample.update(state="confirmed", fill_status=entry["fill_status"],
                              confirmed_at=entry["confirmed_at"],
                              entry_qualification_version=entry["qualification_version"],
                              entry_date=entry["entry_date"], entry_price=entry["entry_price"],
                              round_trip_fee_pct=entry["round_trip_fee_pct"],
                              exit_slippage_pct=entry["exit_slippage_pct"])
                terms = db.execute("SELECT * FROM paper_entry_terms WHERE record_id=?", (record_id,)).fetchone()
                if terms:
                    term_values = dict(terms)
                    terms_valid = _entry_chain_valid(db, record_id, entry, term_values)
                    sample.update(accounting_version=term_values["accounting_version"],
                                  protocol_version=term_values["protocol_version"],
                                  terms_valid=terms_valid, quantity=term_values["quantity"],
                                  entry_fees_cny=term_values["entry_fees_cny"],
                                  total_cost_cny=term_values["total_cost_cny"])
                    if terms_valid:
                        closes = _forward_closes(db, pick["code"], entry["entry_date"], sessions, cutoff)
                        sample["bars"] = [{"date": day, "close": close} for day, close in closes.items()]
                        sample["mark_valid_dates"] = list(closes)
                if entry["fill_status"] == "simulated_fill" and not terms and entry["accounting_version"] == "legacy-percent-fee-v1":
                    factors = {row["trade_date"]: row for row in db.execute(
                        "SELECT * FROM corporate_action_factors WHERE code=? AND trade_date IN (" +
                        ",".join("?" for _ in sessions) + ")", (pick["code"], *sessions),
                    )} if sessions else {}
                    entry_factor = factors.get(entry["entry_date"])
                    def factor_known(row):
                        if not row or row["source"] != "tushare_adj_factor" or row["conflicted"]:
                            return False
                        try:
                            observed = datetime.fromisoformat(row["observed_at"])
                            return bool(observed.tzinfo and observed.astimezone(timezone.utc) <= cutoff
                                        and row["response_sha256"])
                        except (TypeError, ValueError):
                            return False
                    for bar in db.execute(
                        "SELECT trade_date,close,price_basis,corporate_action_factor,corporate_action_evidence "
                        "FROM daily_bars WHERE code=? AND trade_date>? AND trade_date<=? ORDER BY trade_date",
                        (pick["code"], entry["entry_date"], as_of),
                    ):
                        factor = factors.get(bar["trade_date"])
                        comparable = bool(
                            factor_known(entry_factor) and factor_known(factor)
                            and entry_factor["adj_factor"] == factor["adj_factor"]
                            and entry_factor["observed_at"] and factor["observed_at"]
                            and entry_factor["response_sha256"] and factor["response_sha256"]
                            and bar["price_basis"] == "unadjusted"
                            and bar["corporate_action_factor"] == factor["adj_factor"]
                            and bar["corporate_action_evidence"]
                        )
                        sample["bars"].append({"date": bar["trade_date"],
                                               "close": bar["close"] if comparable else None})
            samples.append(sample)
        return review(samples, sessions, as_of=as_of)
    finally:
        db.close()
