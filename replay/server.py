#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Local replay proxy for offline cryptocmd runs.

It serves prerecorded CoinMarketCap JSON fixtures over plain HTTP, keyed
by the same identity the client cache uses -- URL path + canonicalised
query string (see ``cryptocmd.fetching.canonical_key``). Using one shared
identity for fixture lookup and client caching is what guarantees:

* a recorded page always hits its own fixture (no cross-page aliasing);
* an unrecorded page is a real MISS (HTTP 404), never a stale page from
  a neighbour key -- important for the empty/last page, which must hit
  its own (empty-quotes) fixture rather than be confused with a miss;
* concurrent pagination sees the same 1 page <-> 1 key mapping.

Fixtures are regular ``.json`` files under ``--fixture-dir`` with an
``index.json`` mapping canonical key -> relative fixture path.

Flaky fixtures (retry exercise)
--------------------------------
A request carrying ``__flaky=2`` fails its first two serves with HTTP
503, then serves normally. The marker is stripped before the key
lookup, so the same recorded page is used. Every 503 contributes to
the retry count shown on the status page.

Status page
-----------
``GET /__status``  HTML dashboard (auto-refresh), per-key hit/miss/
retry counts and the page interval (timeStart..timeEnd, candle count,
empty-page flag), plus aggregate counters.
``GET /__status?format=json``  machine readable counters.
``POST /__reset``               clear all counters.
"""

from __future__ import print_function

__all__ = ["ReplayHandler", "run"]

import json
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qsl, urlunsplit, urlencode

from cryptocmd.fetching import request_identity


def _strip_marker(query_pairs):
    """Remove replay-only control params before computing the fixture key."""
    flaky_fail = 0
    clean = []
    for k, v in query_pairs:
        if k == "__flaky":
            try:
                flaky_fail = int(v)
            except ValueError:
                flaky_fail = 0
        else:
            clean.append((k, v))
    return clean, flaky_fail


class ReplayState(object):
    """Thread-safe aggregate of served requests."""

    def __init__(self):
        self.lock = threading.Lock()
        self.total_requests = 0
        self.hits = 0
        self.misses = 0
        self.server_errors = 0  # 503s served to trigger client retries
        self.keys = {}  # canonical key -> per-key stats

    def record(self, key, outcome, interval=None, empty=None):
        with self.lock:
            self.total_requests += 1
            if outcome == "hit":
                self.hits += 1
            elif outcome == "miss":
                self.misses += 1
            elif outcome == "retry_fail":
                self.server_errors += 1
            stats = self.keys.setdefault(
                key,
                {"hits": 0, "misses": 0, "retries": 0,
                 "interval": interval, "empty": empty},
            )
            if outcome == "hit":
                stats["hits"] += 1
            elif outcome == "miss":
                stats["misses"] += 1
            elif outcome == "retry_fail":
                stats["retries"] += 1
            if interval and stats["interval"] is None:
                stats["interval"] = interval
            if empty is not None:
                stats["empty"] = empty

    def snapshot(self):
        with self.lock:
            return {
                "total_requests": self.total_requests,
                "hits": self.hits,
                "misses": self.misses,
                "server_errors": self.server_errors,
                "keys": [
                    dict(key=k, **v) for k, v in sorted(self.keys.items())
                ],
            }

    def reset(self):
        with self.lock:
            self.total_requests = self.hits = self.misses = self.server_errors = 0
            self.keys.clear()


class ReplayHandler(BaseHTTPRequestHandler):
    state = ReplayState()
    fixture_dir = os.path.join("replay", "fixtures")
    _index = None
    _flaky_seen = {}
    _index_lock = threading.Lock()

    # -- helpers ----------------------------------------------------------

    def _load_index(self):
        if self._index is None:
            with self._index_lock:
                if self._index is None:
                    with open(os.path.join(self.fixture_dir, "index.json"),
                              encoding="utf-8") as fh:
                        self.__class__._index = json.load(fh)
        return self._index

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _interval_for(self, query_pairs, fixture_payload):
        params = dict(query_pairs)
        interval = None
        empty = None
        if "timeStart" in params and "timeEnd" in params:
            ts = datetime.fromtimestamp(int(params["timeStart"]), tz=timezone.utc)
            te = datetime.fromtimestamp(int(params["timeEnd"]), tz=timezone.utc)
            interval = "{}..{}".format(ts.strftime("%Y-%m-%d"), te.strftime("%Y-%m-%d"))
            try:
                empty = len(fixture_payload["data"]["quotes"]) == 0
            except (KeyError, TypeError):
                empty = None
        return interval, empty

    # -- routing ----------------------------------------------------------

    def do_GET(self):
        parsed = urlsplit(self.path)

        if parsed.path == "/__status":
            query = dict(parse_qsl(parsed.query))
            if query.get("format") == "json":
                self._send_json(self.state.snapshot())
            else:
                self._send_html_status()
            return

        query_pairs, flaky_fail = _strip_marker(
            parse_qsl(parsed.query, keep_blank_values=True)
        )
        clean_query = urlencode(query_pairs)
        key = request_identity(
            urlunsplit(("http", parsed.netloc, parsed.path, clean_query, ""))
        )

        index = self._load_index()
        rel = index.get(key)

        if rel is None:
            self.state.record(key, "miss")
            self._send_json(
                {"status": {"error_code": 404,
                            "error_message": "replay miss: no fixture for " + key}},
                status=404,
            )
            return

        with self._index_lock:
            seen = self._flaky_seen.get(key, 0)
            if seen < flaky_fail:
                self._flaky_seen[key] = seen + 1
                should_fail = True
            else:
                should_fail = False

        if should_fail:
            self.state.record(key, "retry_fail")
            self._send_json(
                {"status": {"error_code": 503, "error_message": "synthetic flake"}},
                status=503,
            )
            return

        with open(os.path.join(self.fixture_dir, rel), encoding="utf-8") as fh:
            payload = json.load(fh)
        interval, empty = self._interval_for(query_pairs, payload)
        self.state.record(key, "hit", interval=interval, empty=empty)
        self._send_json(payload)

    def do_POST(self):
        parsed = urlsplit(self.path)
        if parsed.path == "/__reset":
            with self._index_lock:
                self._flaky_seen.clear()
            self.state.reset()
            self._send_json({"reset": True})
        else:
            self._send_json({"error": "not found"}, status=404)

    # -- status page ------------------------------------------------------

    def _send_html_status(self):
        snap = self.state.snapshot()
        rows = []
        for item in snap["keys"]:
            interval = item.get("interval") or "-"
            empty = ""
            if item.get("empty") is True:
                empty = ' <span class="empty">EMPTY LAST PAGE</span>'
            elif item.get("empty") is False:
                empty = ' <span class="ok">has candles</span>'
            rows.append(
                "<tr><td class='key'>{key}</td><td>{interval}{empty}</td>"
                "<td class='num'>{hits}</td><td class='num miss'>{misses}</td>"
                "<td class='num retry'>{retries}</td></tr>".format(
                    key=item["key"], interval=interval, empty=empty,
                    hits=item["hits"], misses=item["misses"], retries=item["retries"],
                )
            )
        html = """<!doctype html>
<html><head><meta charset="utf-8"><meta http-equiv="refresh" content="2">
<title>replay proxy status</title><style>
body{{font-family:monospace;margin:2em}}
table{{border-collapse:collapse;margin-top:1em}}
td,th{{border:1px solid #999;padding:4px 8px;text-align:left}}
.num{{text-align:right}} .key{{font-size:12px}}
.miss{{color:#b00}} .retry{{color:#c60}} .empty{{color:#b00;font-weight:bold}}
.ok{{color:#070}} .agg{{font-size:1.1em}}
</style></head><body>
<h1>replay proxy status</h1>
<p class="agg">requests: {total} &nbsp;|&nbsp; hits: {hits}
&nbsp;|&nbsp; misses: {misses} &nbsp;|&nbsp; retries(503): {retries}</p>
<table><tr><th>canonical key</th><th>page interval</th><th>hits</th>
<th>misses</th><th>retries</th></tr>
{rows}
</table>
<p><i>auto-refresh 2s</i></p></body></html>""".format(
            total=snap["total_requests"], hits=snap["hits"], misses=snap["misses"],
            retries=snap["server_errors"], rows="\n".join(rows),
        )
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):  # quiet
        pass


def run(host="127.0.0.1", port=8765, fixture_dir=None):
    if fixture_dir:
        ReplayHandler.fixture_dir = fixture_dir
    server = ThreadingHTTPServer((host, port), ReplayHandler)
    print("replay proxy on http://{}:{}  (fixtures: {})".format(
        host, port, ReplayHandler.fixture_dir))
    print("status page: http://{}:{}/__status".format(host, port))
    server.serve_forever()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="cryptocmd replay proxy")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8877)
    parser.add_argument("--fixture-dir", default=os.path.join("replay", "fixtures"))
    args = parser.parse_args()
    run(args.host, args.port, args.fixture_dir)
