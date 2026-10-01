"""Read-only dashboard server with an explicit bind address."""
from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .data import Dashboard


STATIC = Path(__file__).with_name("static")


def create_server(dashboard, port=8765, host="127.0.0.1"):
    bind_host = str(host or "127.0.0.1").strip()
    if not bind_host:
        raise ValueError("host must not be empty")

    class Handler(BaseHTTPRequestHandler):
        def response(self, status, body, content_type="application/json; charset=utf-8"):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Cross-Origin-Resource-Policy", "same-origin")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_GET(self):
            configured = str(self.server.server_address[0])
            valid_hosts = {
                f"{configured}:{self.server.server_port}",
                f"{bind_host}:{self.server.server_port}",
                f"127.0.0.1:{self.server.server_port}",
                f"localhost:{self.server.server_port}",
            }
            if self.headers.get("Host") not in valid_hosts:
                self.response(403, b'{"error":"host_not_allowed"}')
                return
            origin = self.headers.get("Origin")
            if origin and origin not in {"http://" + host for host in valid_hosts}:
                self.response(403, b'{"error":"origin_not_allowed"}')
                return
            path = urlsplit(self.path)
            if path.path.startswith("/api/"):
                params = {key: value[0] for key, value in parse_qs(path.query).items()}
                result = dashboard.query(path.path[5:], params)
                try:
                    body = json.dumps(result, ensure_ascii=False, allow_nan=False).encode("utf-8")
                except (TypeError, ValueError):
                    body = b'{"meta":{"status":"unavailable","reason":"invalid_source_values","read_only":true},"data":null}'
                self.response(200, body)
                return
            relative = "index.html" if path.path == "/" else path.path.lstrip("/")
            target = (STATIC / relative).resolve()
            if not target.is_relative_to(STATIC.resolve()) or not target.is_file():
                self.response(404, b'{"error":"not_found"}')
                return
            self.response(200, target.read_bytes(), mimetypes.guess_type(target)[0] or "application/octet-stream")

        # Preserve GET route/status/header behavior while response() suppresses
        # the body for HEAD.
        do_HEAD = do_GET

        def do_POST(self):
            self.response(405, b'{"error":"read_only"}')

        do_PUT = do_POST
        do_PATCH = do_POST
        do_DELETE = do_POST

        def log_message(self, fmt, *args):
            return

    return ThreadingHTTPServer((bind_host, port), Handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--origin", default="", help="Fixed authorized origin; blank exposes public records only")
    parser.add_argument("--settings", type=Path, help="Explicit sanitized public settings snapshot, never a credential file")
    parser.add_argument("--artifact", type=Path, help="Explicit read-only intraday target quote artifact path")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address; defaults to IPv4 loopback")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    if str(args.database).startswith("\\\\"):
        parser.error("use an explicit local database, not a network share")
    server = create_server(Dashboard(args.database, origin=args.origin, settings=args.settings, artifact_path=args.artifact), args.port, args.host)
    print(f"Stock Watch read-only dashboard: http://{args.host}:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
