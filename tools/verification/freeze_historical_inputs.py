"""Freeze outcome-free candidate inputs for a retrospective audit."""

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import sys


def encode(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode("utf-8")


def freeze(report, index):
    if report.get("mode") != "retrospective_only_not_point_in_time":
        raise ValueError("expected retrospective report")
    if report.get("strategy") != "core.apply_daily_indicators + core.score_quote, technical only; top amount deep screen":
        raise ValueError("unexpected selection strategy")
    scope = index.get("scope", {})
    if scope.get("as_of") != report.get("as_of") or scope.get("sample_kind") != "official_index_only":
        raise ValueError("announcement index scope mismatch")
    indexed = {entry["code"]: entry for entry in index.get("codes", [])}
    days = report.get("days", [])
    dates = [day["date"] for day in days]
    if not dates or len(set(dates)) != len(dates) or dates != sorted(dates):
        raise ValueError("selection dates missing, duplicate or out of order")
    if dates[0] != report["as_of"] or dates[-1] != report["through"]:
        raise ValueError("selection endpoints mismatch")
    if len(indexed) != len(index.get("codes", [])):
        raise ValueError("duplicate index code")
    packets = []
    for day in days:
        cutoff = date.fromisoformat(day["date"])
        candidates = []
        for candidate in day["candidates"]:
            code = candidate["code"]
            if not (isinstance(code, str) and len(code) == 6 and code.isdigit()):
                raise ValueError("invalid candidate code")
            if candidate["risk"] != "unknown":
                raise ValueError("unexpected risk in technical report")
            evidence = {"status": "not_collected", "records": [],
                        "publication_time_verified": False,
                        "original_documents_verified": False,
                        "correction_chain_verified": False}
            if day["date"] == scope["as_of"]:
                entry = indexed.get(code)
                if entry is None or not entry["status"].startswith("complete"):
                    raise ValueError(f"first-day index not reconciled: {code}")
                seen = {}
                records = []
                for record in entry["records"]:
                    published = date.fromisoformat(record["published_at"])
                    if published >= cutoff:
                        continue
                    identifier = record["announcement_id"]
                    if identifier in seen:
                        if seen[identifier] != (published, record["source"]):
                            raise ValueError(f"conflicting announcement: {code}/{identifier}")
                        continue
                    seen[identifier] = (published, record["source"])
                    records.append({"announcement_id": identifier,
                                    "published_date": published.isoformat(),
                                    "document_url": record["source"]})
                evidence["status"] = "retrospective_index_rows_partial_not_as_of_verified" if len({record["announcement_id"] for record in entry["records"]}) != entry["unique_announcement_ids"] else "retrospective_index_only_not_as_of_verified"
                evidence["records"] = sorted(records, key=lambda item: (item["published_date"], item["announcement_id"]))
            candidates.append({"code": code, "technical_score": candidate["score"],
                               "as_of_unadjusted_close": candidate["as_of_close"],
                               "history_days": candidate["history_days"],
                               "technical_risk": "unknown", "announcement_evidence": evidence,
                               "reasoning_status": "not_run"})
        if len({item["code"] for item in candidates}) != len(candidates):
            raise ValueError(f"duplicate candidate: {day['date']}")
        packets.append({"schema_version": "historical_reasoning_input.v1",
                        "date": day["date"],
                        "cutoff_policy": "after daily close; same-day announcements excluded without exact timestamp",
                        "selection_status": day["status"],
                        "history_coverage": day["history_coverage"],
                        "selection_is_point_in_time_proven": False,
                        "market_status_verified": False,
                        "candidates": candidates})
    return packets


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--index", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        report_bytes = args.report.read_bytes()
        index_bytes = args.index.read_bytes()
        packets = freeze(json.loads(report_bytes), json.loads(index_bytes))
        if args.output_dir.exists():
            raise ValueError("output already exists; refusing to overwrite frozen inputs")
        args.output_dir.mkdir(parents=True)
        hashes = {}
        for packet in packets:
            name = f"{packet['date']}.json"
            data = encode(packet)
            (args.output_dir / name).write_bytes(data)
            hashes[name] = hashlib.sha256(data).hexdigest()
        manifest = {"schema_version": "historical_reasoning_manifest.v1",
                    "source_report_sha256": hashlib.sha256(report_bytes).hexdigest(),
                    "source_index_sha256": hashlib.sha256(index_bytes).hexdigest(),
                    "packet_sha256": hashes,
                    "candidate_events": sum(len(packet["candidates"]) for packet in packets),
                    "reasoning_completed": 0, "future_outcomes_not_copied": True}
        (args.output_dir / "manifest.json").write_bytes(encode(manifest))
        print(json.dumps({"output_dir": str(args.output_dir), "days": len(packets),
                          "candidates": manifest["candidate_events"], "reasoning_completed": 0}))
        return 0
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"input freeze not completed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
