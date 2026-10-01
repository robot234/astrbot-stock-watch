"""Stage a bounded set of pre-cutoff official PDFs for blind evidence review."""

import argparse
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from pypdf import PdfReader


def run(packet, index, output, *, per_code=3, exclude=(), max_seconds=360):
    if output.exists() or not 1 <= per_code <= 6 or max_seconds < 1:
        raise ValueError("new output directory and bounded documents per code required")
    if packet["date"] != index["scope"]["as_of"] or index["scope"]["sample_kind"] != "official_index_only":
        raise ValueError("frozen selection and announcement index mismatch")
    listed = {item["code"]: item for item in index["codes"]}
    output.mkdir(parents=True)
    results = []
    deadline = time.monotonic() + max_seconds
    for candidate in packet["candidates"]:
        code = candidate["code"]
        if code in exclude:
            continue
        entry = listed.get(code)
        if not entry:
            raise ValueError(f"announcement index absent for {code}")
        deduplicated = {}
        for record in entry["records"]:
            if record["published_at"] >= packet["date"]:
                continue
            identifier = record["announcement_id"]
            if identifier in deduplicated and (record["published_at"], record["source"]) != (
                deduplicated[identifier]["published_at"], deduplicated[identifier]["source"]
            ):
                raise ValueError(f"conflicting index entries for {identifier}")
            deduplicated[identifier] = record
        selected = sorted(deduplicated.values(), key=lambda record: (record["published_at"], record["announcement_id"]), reverse=True)[:per_code]
        for record in selected:
            identifier, url = record["announcement_id"], record["source"]
            if urlsplit(url).hostname != "static.cninfo.com.cn":
                raise ValueError("non-official announcement URL")
            document = {"code": code, "announcement_id": identifier,
                        "index_date": record["published_at"], "source_url": url,
                        "index_status": entry["status"], "historical_visibility_proven": False,
                        "exact_publication_time": "unknown", "correction_chain": "unknown"}
            if time.monotonic() >= deadline:
                document["status"] = "not_attempted_time_budget"
                results.append(document)
                continue
            attempts = []
            for attempt in range(2):
                try:
                    with urlopen(Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=30) as response:
                        if urlsplit(response.url).hostname != "static.cninfo.com.cn":
                            raise ValueError("redirect outside official source")
                        content = response.read(10 * 1024 * 1024 + 1)
                        if response.status != 200 or len(content) > 10 * 1024 * 1024 or not content.startswith(b"%PDF-"):
                            raise ValueError("unexpected response format or size")
                    destination = output / f"{code}-{identifier}.pdf"
                    destination.write_bytes(content)
                    pages = [page.extract_text() or "" for page in PdfReader(destination).pages]
                    text = output / f"{code}-{identifier}.txt"
                    text.write_text("\n\n".join(pages), encoding="utf-8")
                    document.update({"status": "extracted", "bytes": len(content),
                                     "sha256": hashlib.sha256(content).hexdigest(),
                                     "pages": len(pages), "text_characters": sum(map(len, pages)),
                                     "pdf_path": str(destination), "text_path": str(text)})
                    break
                except Exception as error:
                    attempts.append(f"{type(error).__name__}: {str(error)[:160]}")
                    if attempt == 0:
                        time.sleep(1)
            else:
                document["status"] = "unknown_download_or_extraction"
            document["attempt_errors"] = attempts
            results.append(document)
    manifest = {"schema_version": "single_cohort_announcements.v1",
                "as_of": packet["date"], "selection_was_repeated": False,
                "source_scope": "retrospective official index and retrieved original PDFs; not a historical publication snapshot",
                "documents": results}
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"code_count": len({item["code"] for item in results}),
            "selected": len(results), "extracted": sum(item["status"] == "extracted" for item in results),
            "manifest": str(output / "manifest.json")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--exclude", nargs="*", default=[])
    parser.add_argument("--per-code", type=int, default=3)
    parser.add_argument("--max-seconds", type=int, default=360)
    args = parser.parse_args()
    print(json.dumps(run(json.loads(args.packet.read_text(encoding="utf-8")),
                         json.loads(args.index.read_text(encoding="utf-8")), args.output,
                         per_code=args.per_code, exclude=args.exclude,
                         max_seconds=args.max_seconds), ensure_ascii=False))


if __name__ == "__main__":
    main()
