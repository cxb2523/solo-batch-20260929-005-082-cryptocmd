#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""本地回放服务（离线时代替外网 CMC 接口）。

路由：
  /data-api/v3/...      按 request_key(path+排序查询串) 查 manifest 返回夹具；
                        命中 200，未命中 503（X-Replay-Status: miss），
                        503 会被客户端重试层重试，门控据此统计重试次数。
  /__replay/status      可刷新的 HTML 状态页（2 秒自动 refresh）
  /__replay/state       同样数据的 JSON，供脚本断言

状态页逐键汇总：命中次数、总尝试次数、重试次数、首/末次请求时间、页区间
（timeStart..timeEnd 对应的 UTC 日期）、本页 quotes 条数。

注意：夹具索引用的是 cryptocmd.utils.request_key，与客户端缓存装饰器
完全同一口径——这是命中率统计可信的前提。
"""

from __future__ import print_function

import argparse
import datetime
import html
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import sys
import threading
from urllib.parse import parse_qsl, urlsplit

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from cryptocmd.utils import request_key  # noqa: E402

REPLAY_DIR = os.path.join(ROOT, "replay")

_PREFIX = "/data-api/"


class ReplayState:
    """线程安全的逐键请求计数。"""

    def __init__(self):
        self._lock = threading.Lock()
        self.rows = {}
        self.started_at = datetime.datetime.now(datetime.timezone.utc)

    def record(self, key, hit, quotes=None, interval=None):
        with self._lock:
            row = self.rows.setdefault(
                key,
                {
                    "key": key,
                    "hits": 0,
                    "misses": 0,
                    "attempts": 0,
                    "first": None,
                    "last": None,
                    "quotes": 0,
                    "interval": interval or "-",
                },
            )
            now = datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S UTC")
            if row["first"] is None:
                row["first"] = now
            row["last"] = now
            row["attempts"] += 1
            row["hits"] += 1 if hit else 0
            row["misses"] += 0 if hit else 1
            if quotes is not None:
                row["quotes"] = quotes
            if interval:
                row["interval"] = interval

    def snapshot(self):
        with self._lock:
            rows = [dict(row) for row in self.rows.values()]
        for row in rows:
            # 同一逻辑请求的重复尝试即"重试"（首次尝试不计）。
            row["retries"] = max(0, row["attempts"] - 1)
        return rows


def _page_interval(path_query):
    """从 historical URL 的查询参数推导页区间 [timeStart, timeEnd]（UTC 日期）。"""
    query = dict(parse_qsl(urlsplit(path_query).query))
    try:
        start = int(query["timeStart"])
        end = int(query["timeEnd"])
    except (KeyError, ValueError):
        return "-"
    start_day = datetime.datetime.fromtimestamp(start, datetime.timezone.utc).strftime(
        "%Y-%m-%d"
    )
    end_day = datetime.datetime.fromtimestamp(end, datetime.timezone.utc).strftime(
        "%Y-%m-%d"
    )
    return "{} .. {}".format(start_day, end_day)


def build_handler(state, fixtures, manifest_keys):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # 安静；计数看状态页
            pass

        def _send(self, status, payload, extra_headers=None):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for key, value in (extra_headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlsplit(self.path)
            if parsed.path == "/__replay/state":
                self._send(200, {"rows": state.snapshot()})
                return
            if parsed.path == "/__replay/status":
                body = render_status_page(state).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            # host 已被客户端改写，path+query 里仍保留 /data-api/v3/... 原样。
            key = request_key(self.path)
            fixture_rel = manifest_keys.get(key)
            interval = _safe_page_interval(self.path)
            if fixture_rel is None:
                state.record(key, False, interval=interval)
                self._send(
                    503,
                    {
                        "status": {
                            "error_code": 503,
                            "error_message": "replay miss: no fixture for " + key,
                        }
                    },
                    {"X-Replay-Status": "miss"},
                )
                return
            payload = fixtures[fixture_rel]
            quotes = len(payload.get("data", {}).get("quotes", []))
            state.record(key, True, quotes=quotes, interval=interval)
            self._send(200, payload, {"X-Replay-Status": "hit"})

    return Handler


def _safe_page_interval(path_query):
    try:
        return _page_interval(path_query)
    except Exception:
        return "-"


def render_status_page(state):
    rows = sorted(state.snapshot(), key=lambda r: r["first"] or "")
    total_hits = sum(r["hits"] for r in rows)
    total_misses = sum(r["misses"] for r in rows)
    total_attempts = sum(r["attempts"] for r in rows)
    total_retries = sum(r["retries"] for r in rows)

    rows_html = []
    for row in rows:
        status = "HIT" if row["hits"] else "MISS"
        color = "#1a7f37" if row["hits"] else "#cf222e"
        rows_html.append(
            "<tr>"
            "<td>{first}</td><td>{last}</td>"
            "<td style='color:{color};font-weight:bold'>{status}</td>"
            "<td style='text-align:right'>{hits}</td>"
            "<td style='text-align:right'>{misses}</td>"
            "<td style='text-align:right'>{attempts}</td>"
            "<td style='text-align:right'>{retries}</td>"
            "<td>{interval}</td>"
            "<td style='text-align:right'>{quotes}</td>"
            "<td style='font-family:monospace;font-size:12px'>{key}</td>"
            "</tr>".format(
                first=row["first"],
                last=row["last"],
                color=color,
                status=status,
                hits=row["hits"],
                misses=row["misses"],
                attempts=row["attempts"],
                retries=row["retries"],
                interval=row["interval"],
                quotes=row["quotes"],
                key=html.escape(row["key"]),
            )
        )

    def _row_time_end(row):
        for pair in row["key"].split("?", 1)[1].split("&"):
            if pair.startswith("timeEnd="):
                try:
                    return int(pair.split("=", 1)[1])
                except ValueError:
                    return -1
        return -1

    # 末页按时间窗而非请求到达顺序选取（并发下到达顺序不代表页序）。
    hist_rows = [r for r in rows if "historical" in r["key"]]
    last_row = max(hist_rows, key=_row_time_end, default=None)
    last_interval = last_row["interval"] if last_row else "-"
    last_quotes = last_row["quotes"] if last_row else "-"

    return """<!doctype html>
<html><head><meta charset="utf-8">
<meta http-equiv="refresh" content="2">
<title>replay status</title>
<style>
body {{ font-family: -apple-system, Segoe UI, sans-serif; margin: 24px; }}
table {{ border-collapse: collapse; margin-top: 12px; }}
th, td {{ border: 1px solid #ccc; padding: 4px 8px; font-size: 13px; }}
th {{ background: #f0f0f0; }}
.summary span {{ display: inline-block; margin-right: 24px; font-size: 15px; }}
</style></head><body>
<h2>cryptoCMD replay proxy &mdash; live status</h2>
<div class="summary">
<span><b>Hits:</b> {hits}</span>
<span><b>Misses:</b> {misses}</span>
<span><b>Attempts:</b> {attempts}</span>
<span><b>Retries:</b> {retries}</span>
<span><b>Last page interval:</b> {last_interval} ({last_quotes} quotes)</span>
</div>
<table>
<tr><th>first</th><th>last</th><th>status</th><th>hits</th><th>misses</th>
<th>attempts</th><th>retries</th><th>page interval</th><th>quotes</th><th>request key</th></tr>
{rows}
</table>
<p style="color:#666;font-size:12px">Auto-refresh every 2s. Keys are path + sorted query params (same fingerprint the client cache uses).</p>
</body></html>""".format(
        hits=total_hits,
        misses=total_misses,
        attempts=total_attempts,
        retries=total_retries,
        last_interval=html.escape(str(last_interval)),
        last_quotes=last_quotes,
        rows="\n".join(rows_html),
    )


def load_store():
    with open(os.path.join(REPLAY_DIR, "manifest.json"), encoding="utf-8") as handle:
        manifest = json.load(handle)
    fixtures = {}
    for key, rel in manifest["keys"].items():
        with open(os.path.join(REPLAY_DIR, rel), encoding="utf-8") as handle:
            fixtures[rel] = json.load(handle)
    return fixtures, manifest["keys"]


def start_server(port=0):
    fixtures, manifest_keys = load_store()
    state = ReplayState()
    server = ThreadingHTTPServer(
        ("127.0.0.1", port), build_handler(state, fixtures, manifest_keys)
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, state


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    server, _ = start_server(args.port)
    print("replay server listening on http://127.0.0.1:{}".format(args.port))
    print("status page: http://127.0.0.1:{}/__replay/status".format(args.port))
    try:
        thread_event = threading.Event()
        thread_event.wait()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
