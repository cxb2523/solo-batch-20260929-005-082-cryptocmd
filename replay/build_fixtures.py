#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate deterministic offline fixtures for the replay proxy.

The fixtures are *synthesised* (no live network) but their request keys
are produced with the very same code the scraper uses to generate page
URLs (``iter_time_chunks`` + ``build_historical_url`` +
``canonical_key``). Key generation and slicing therefore cannot drift:
the pages the scraper requests are exactly the entries written to
index.json -- that alignment is what makes the offline hit rate
predictable and keeps the empty last page a distinct, hittable key
rather than a miss.

Produced scenario (coin_id=1 / BTC, convert=2781 / USD):

* listing fixture (code -> id resolution);
* a multi-page range whose FINAL page has zero quotes, exercising the
  last/empty-page boundary through the same key as a normal page.
"""

from __future__ import print_function

import json
import os
import shutil
import sys
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cryptocmd.fetching import request_identity
from cryptocmd.utils import (
    parse_date,
    iter_time_chunks,
    build_historical_url,
    build_listing_url,
)

# The scraper rewrites every URL (path + query) onto this base, so the
# base must carry the same /data-api/v3 prefix the live API uses.
API_BASE = "http://127.0.0.1/data-api/v3"
COIN_ID = 1
CONVERT_ID = 2781
START = "01-01-2023"
# Range spans two full 365-day pages (2023 and leap-year 2024 up to
# 2024-12-30) plus an overflow tail; the tail lands on a third page
# whose fixture is recorded EMPTY, exercising the empty-last-page
# boundary through its own distinct key.
END = "05-01-2025"
EMPTY_OVERFLOW_DAYS = 6
FIXTURE_DIR = os.path.join("replay", "fixtures")


def make_quote(day, index):
    price = 100.0 + index
    return {
        "timeOpen": "{}T00:00:00.000Z".format(day.strftime("%Y-%m-%d")),
        "timeHigh": "{}T12:00:00.000Z".format(day.strftime("%Y-%m-%d")),
        "timeLow": "{}T06:00:00.000Z".format(day.strftime("%Y-%m-%d")),
        "timeClose": "{}T23:59:59.999Z".format(day.strftime("%Y-%m-%d")),
        "quote": {
            "open": price,
            "high": price + 1,
            "low": price - 1,
            "close": price + 0.5,
            "volume": 1000.0,
            "marketCap": 1000000.0,
        },
    }


def response(quotes):
    return {
        "status": {"error_code": 0, "error_message": None, "timestamp": "2025-01-05T00:00:00.000Z"},
        "data": {"id": COIN_ID, "symbol": "BTC", "name": "Bitcoin", "quotes": quotes},
    }


def main():
    if os.path.isdir(FIXTURE_DIR):
        shutil.rmtree(FIXTURE_DIR)
    os.makedirs(FIXTURE_DIR)

    index = {}

    # listing
    listing = {
        "status": {"error_code": 0, "error_message": None},
        "data": {
            "cryptoCurrencyList": [
                {"id": COIN_ID, "symbol": "BTC", "name": "Bitcoin"},
            ]
        },
    }
    listing_url = build_listing_url(API_BASE)
    index[request_identity(listing_url)] = "listing.json"
    with open(os.path.join(FIXTURE_DIR, "listing.json"), "w", encoding="utf-8") as fh:
        json.dump(listing, fh, indent=2)

    # historical pages: every page except the final overflow page holds
    # daily candles; the final page is intentionally recorded EMPTY.
    start_dt = parse_date(START)
    wanted_end_dt = parse_date(END)
    end_dt = wanted_end_dt + datetime.timedelta(days=EMPTY_OVERFLOW_DAYS)
    pages = list(iter_time_chunks(start_dt, end_dt))

    for n, (ts, te) in enumerate(pages, start=1):
        url = build_historical_url(API_BASE, COIN_ID, CONVERT_ID, ts, te)
        # CMC timeStart is exclusive -> first candle day = ts + 1
        first_day = ts.date() + __import__("datetime").timedelta(days=1)
        last_day = min(te, wanted_end_dt).date()
        is_last_page = n == len(pages)

        quotes = []
        if not is_last_page:
            day = first_day
            i = 0
            while day <= last_day:
                quotes.append(make_quote(day, (n - 1) * 400 + i))
                day += __import__("datetime").timedelta(days=1)
                i += 1

        name = "historical_{}.json".format(n)
        index[request_identity(url)] = name
        with open(os.path.join(FIXTURE_DIR, name), "w", encoding="utf-8") as fh:
            json.dump(response(quotes), fh, indent=2)
        print("page {}: {}..{} -> {} ({} quotes{})".format(
            n,
            first_day.strftime("%Y-%m-%d"),
            last_day.strftime("%Y-%m-%d"),
            name,
            len(quotes),
            ", EMPTY LAST PAGE" if is_last_page and not quotes else "",
        ))

    with open(os.path.join(FIXTURE_DIR, "index.json"), "w", encoding="utf-8") as fh:
        json.dump(index, fh, indent=2, sort_keys=True)

    print("wrote {} fixture entries to {}".format(len(index), FIXTURE_DIR))


if __name__ == "__main__":
    main()
