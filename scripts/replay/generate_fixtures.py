#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""生成离线回放夹具。

夹具与客户端"切片口径"必须同源：这里直接调用 cryptocmd.utils 的纯函数
request_key / iter_page_windows / build_*_url，所以生成出来的键和
离线运行时缓存层、回放服务查找用的键永远一致。

场景设计（CRYPTOCMD_PAGE_DAYS=5, id=9001, convertId=2781 USD）：
  页1 timeEnd=2026-01-05  满页 5 条
  页2 timeEnd=2026-01-10  空页 0 条  ← 中间空页不得让下载提前终止
  页3 timeEnd=2026-01-12  短末页 2 条 ← 剩余天数 < page_days
另含一条 listing 夹具（symbol HIT -> id 9001）。

故意不生成 id=9999 的任何夹具：offline-gate 用它验证"未命中"路径
（回放服务返回 503 -> 重试耗尽 -> FetchError，有明确输出）。
"""

from __future__ import print_function

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from cryptocmd.utils import (  # noqa: E402
    build_historical_url,
    build_listing_url,
    iter_page_windows,
    parse_date,
    request_key,
    to_utc_timestamp,
)

REPLAY_DIR = os.path.join(ROOT, "replay")
FIXTURE_DIR = os.path.join(REPLAY_DIR, "fixtures")

API_BASE = "https://api.coinmarketcap.com/data-api/v3"
COIN_ID = 9001
CONVERT_ID = 2781  # USD in fiat_ids.json
START = "01-01-2026"
END = "12-01-2026"
PAGE_DAYS = 5

# 每个页窗内哪些天有行情。timeStart 是排他的，所以首日 = 窗口第一天。
PAGE_QUOTE_DAYS = {
    to_utc_timestamp(parse_date("05-01-2026")): [1, 2, 3, 4, 5],  # 满页
    to_utc_timestamp(parse_date("10-01-2026")): [],  # 空页
    to_utc_timestamp(parse_date("12-01-2026")): [11, 12],  # 短末页
}


def quote(day, price):
    date_str = day.strftime("%Y-%m-%d")
    return {
        "timeOpen": f"{date_str}T00:00:00.000Z",
        "timeHigh": f"{date_str}T12:00:00.000Z",
        "timeLow": f"{date_str}T06:00:00.000Z",
        "timeClose": f"{date_str}T23:59:59.999Z",
        "quote": {
            "open": price,
            "high": price + 100.0,
            "low": price - 100.0,
            "close": price + 50.0,
            "volume": 1000000.0,
            "marketCap": 900000000.0,
        },
    }


def historical_response(quotes):
    return {
        "status": {
            "error_code": 0,
            "error_message": None,
            "timestamp": "2026-01-12T00:00:00.000Z",
        },
        "data": {
            "id": COIN_ID,
            "symbol": "HIT",
            "name": "HitFixtoken",
            "quotes": quotes,
        },
    }


def listing_response():
    return {
        "status": {"error_code": 0, "error_message": None},
        "data": {
            "cryptoCurrencyList": [
                {"id": COIN_ID, "symbol": "HIT", "name": "HitFixtoken"},
            ]
        },
    }


def main():
    os.makedirs(FIXTURE_DIR, exist_ok=True)
    manifest = {"version": 1, "keys": {}}

    def add(url, payload):
        key = request_key(url)
        fname = f"{len(manifest['keys']):03d}.json"
        with open(os.path.join(FIXTURE_DIR, fname), "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
        manifest["keys"][key] = f"fixtures/{fname}"
        print(
            "fixture {:<14} {} ({} quotes)".format(
                fname, key, len(payload.get("data", {}).get("quotes", []))
            )
        )

    add(build_listing_url(API_BASE), listing_response())

    windows = list(iter_page_windows(parse_date(START), parse_date(END), PAGE_DAYS))
    assert len(windows) == 3, f"expected 3 pages (full/empty/short), got {len(windows)}"
    for win_start, win_end in windows:
        url = build_historical_url(API_BASE, COIN_ID, CONVERT_ID, win_start, win_end)
        days = PAGE_QUOTE_DAYS[to_utc_timestamp(win_end)]
        quotes = [quote(win_end.replace(day=d), 100.0 + d) for d in days]
        add(url, historical_response(quotes))

    with open(
        os.path.join(REPLAY_DIR, "manifest.json"), "w", encoding="utf-8"
    ) as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")

    print("manifest written: {} keys".format(len(manifest["keys"])))


if __name__ == "__main__":
    main()
