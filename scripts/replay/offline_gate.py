#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""离线门控：拔网线后的端到端验证（make offline-gate）。

流程：
1. 安装 socket 守卫：任何非 loopback 的真实连接一律拒绝（代理环境变量也清掉，
   防止 requests 走代理"偷偷联网"）。先装守卫后 import cryptocmd，确保全程离线。
2. 本地起回放服务，CRYPTOCMD_API_BASE 指向它。
3. 未命中路径：抓 id=9999 的单页区间（无夹具），预期 503 x 3 次尝试
   （1 请求 + 2 重试）后 FetchError。
4. 命中路径：抓 HIT 2026-01-01..12（page_days=5 → 满页/空页/短末页），
   连续抓两次——第二次必须完全命中进程缓存（回放服务计数不再增长）。
5. 拉 /__replay/state + /__replay/status 核对：
   hits=4 misses=3 retries=2，末页区间 2026-01-10..2026-01-12、quotes=2。

守卫装完后服务保持可访问（127.0.0.1），--keep-alive 默认给 20 秒时间
在浏览器里打开状态页人工复核；--keep-alive 0 用于自动化。
"""

from __future__ import print_function

import argparse
import json
import os
import socket
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts", "replay"))

_LOOPBACK_IPV4 = ("127.0.0.1", "localhost")
_LOOPBACK_IPV6 = ("::1",)


def install_network_guard():
    """拒绝一切外网连接；只允许 loopback（回放服务与状态页都在本地）。"""
    real_connect = socket.socket.connect

    def guarded_connect(self, address):
        host = address[0] if isinstance(address, tuple) else ""
        if host in _LOOPBACK_IPV4 or host in _LOOPBACK_IPV6:
            return real_connect(self, address)
        raise OSError("network unplugged (offline gate): blocked {}".format(host))

    socket.socket.connect = guarded_connect

    def guarded_create_connection(address, *args, **kwargs):
        host = address[0] if isinstance(address, tuple) else ""
        if host not in _LOOPBACK_IPV4 and host not in _LOOPBACK_IPV6:
            raise OSError("network unplugged (offline gate): blocked {}".format(host))
        return socket.create_connection.__wrapped__(address, *args, **kwargs)

    guarded_create_connection.__wrapped__ = socket.create_connection
    socket.create_connection = guarded_create_connection

    for var in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        os.environ.pop(var, None)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--keep-alive",
        type=int,
        default=20,
        help="seconds to leave the replay server up after checks "
        "so the status page can be opened (default 20)",
    )
    parser.add_argument(
        "--server-port",
        type=int,
        default=None,
        help="attach to an already running replay server on this port "
        "instead of starting an in-process one",
    )
    args = parser.parse_args(argv)

    print("== installing network guard (non-loopback connections blocked) ==")
    install_network_guard()

    import server as replay_server  # noqa: E402

    if args.server_port is None:
        httpd, _state = replay_server.start_server(0)
        port = httpd.server_address[1]
    else:
        httpd, port = None, args.server_port
    base = "http://127.0.0.1:{}/data-api/v3".format(port)
    status_url = "http://127.0.0.1:{}/__replay/status".format(port)
    state_url = "http://127.0.0.1:{}/__replay/state".format(port)

    print("== replay server: {} ==".format(base))

    # 必须在 import cryptocmd 之前配好：fetcher 栈在模块导入时按环境构建。
    os.environ["CRYPTOCMD_API_BASE"] = base
    os.environ["CRYPTOCMD_PAGE_DAYS"] = "5"
    os.environ["CRYPTOCMD_RETRIES"] = "3"
    os.environ["CRYPTOCMD_MAX_WORKERS"] = "4"

    from cryptocmd import CmcScraper  # noqa: E402
    from cryptocmd.utils import FetchError  # noqa: E402

    # ---- 未命中路径（先跑，保证状态页最后一条 historical 行是命中的短末页）----
    # 只请求单页区间（01..05 恰好一页），因此只有 1 个缺失键，
    # 3 次尝试 = 1 次请求 + 2 次重试，计数确定。
    print("\n== MISS PATH: id_number=9999 has no fixture ==")
    miss_error = None
    try:
        CmcScraper(
            None, "01-01-2026", "05-01-2026", id_number=9999, max_workers=4
        ).get_data()
    except FetchError as err:
        miss_error = err
        print("[MISS] raised FetchError after retries as expected: {}".format(err))
    except Exception as err:  # noqa: BLE001
        miss_error = err
        print(
            "[MISS] raised {} after retries as expected: {}".format(
                type(err).__name__, err
            )
        )
    if miss_error is None:
        print("[MISS] FAIL: expected a replay-miss failure, got success")
        return 1

    # ---- 命中路径 ----
    print("\n== HIT PATH: HIT 01-01-2026..12-01-2026 (full/empty/short pages) ==")
    scraper = CmcScraper("HIT", "01-01-2026", "12-01-2026", max_workers=4)
    _headers, rows = scraper.get_data()
    dates = sorted(row[0] for row in rows)
    print("[HIT] first download: {} rows; dates={}".format(len(rows), dates))

    # 第二次下载：每一页都应被客户端缓存拦下，回放服务计数不再增加。
    before = json.load(urllib.request.urlopen(state_url))
    _headers2, rows2 = CmcScraper(
        "HIT", "01-01-2026", "12-01-2026", max_workers=4
    ).get_data()
    after = json.load(urllib.request.urlopen(state_url))
    print(
        "[HIT] second download: {} rows (must be served from client cache)".format(
            len(rows2)
        )
    )

    # ---- 状态页核对 ----
    print("\n== STATE PAGE CHECK ==")
    rows_state = after["rows"]
    total_hits = sum(r["hits"] for r in rows_state)
    total_misses = sum(r["misses"] for r in rows_state)
    total_retries = sum(r["retries"] for r in rows_state)
    for row in sorted(rows_state, key=lambda r: r["first"] or ""):
        print(
            "  {:4s} hits={} misses={} attempts={} retries={} interval={} "
            "quotes={} {}".format(
                "HIT" if row["hits"] else "MISS",
                row["hits"],
                row["misses"],
                row["attempts"],
                row["retries"],
                row["interval"],
                row["quotes"],
                row["key"],
            )
        )

    def _time_end(row):
        for part in row["key"].split("?")[1].split("&"):
            if part.startswith("timeEnd="):
                return int(part.split("=", 1)[1])
        return -1

    # 并发翻页时请求到达顺序不定，末页只能按时间窗 (timeEnd 最大) 判定，
    # 不能取"最后记录到的那条"，否则会把并发晚到的前页误当末页。
    last_hist = max(
        (r for r in rows_state if "historical" in r["key"] and r["hits"]),
        key=_time_end,
    )
    html_page = urllib.request.urlopen(status_url).read().decode("utf-8")

    checks = [
        ("hit path returned 7 rows (5 + 0 empty + 2 short)", len(rows) == 7),
        ("both downloads return identical data", rows == rows2),
        (
            "second download added zero replay requests",
            sum(r["attempts"] for r in before["rows"])
            == sum(r["attempts"] for r in after["rows"]),
        ),
        ("total hits == 4 (listing + 3 pages)", total_hits == 4),
        ("total misses == 3 (1 missing key x 3 attempts)", total_misses == 3),
        ("total retries == 2 (attempts - 1 per request key)", total_retries == 2),
        (
            "last page interval is 2026-01-10 .. 2026-01-12",
            last_hist["interval"] == "2026-01-10 .. 2026-01-12",
        ),
        ("last (short) page carries 2 quotes", last_hist["quotes"] == 2),
        (
            "empty middle page did not stop pagination",
            any(r["hits"] and "timeEnd=1768003200" in r["key"] for r in rows_state),
        ),
        (
            "HTML status page shows last interval",
            "2026-01-10 .. 2026-01-12" in html_page,
        ),
        (
            "HTML status page shows both HIT and MISS",
            ">HIT<" in html_page and ">MISS<" in html_page,
        ),
    ]

    print()
    ok = True
    for label, passed in checks:
        print("  [{}] {}".format("OK" if passed else "FAIL", label))
        ok = ok and passed

    snapshot_path = os.path.join(ROOT, "replay", "status_snapshot.html")
    with open(snapshot_path, "w", encoding="utf-8") as handle:
        handle.write(html_page)

    print("\nstatus page (auto-refreshing): {}".format(status_url))
    print("snapshot saved to: {}".format(snapshot_path))

    if not ok:
        if httpd is not None:
            httpd.shutdown()
        return 1

    if args.keep_alive:
        print(
            "\nkeeping replay server up for {}s — open the status page above "
            "(Ctrl-C to stop early)".format(args.keep_alive)
        )
        try:
            time.sleep(args.keep_alive)
        except KeyboardInterrupt:
            pass
    if httpd is not None:
        httpd.shutdown()
    print("\nOFFLINE GATE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
