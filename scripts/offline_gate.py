#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Offline gate: prove the scraper works with the cable unplugged.

It (re)starts expectations against the local replay proxy only:

1. HIT path  -- a full multi-page download, including a recorded EMPTY
   last page, must succeed and return the concatenated fixture rows.
2. RETRY path -- a flaky page (__flaky=2) must be transparently
   retried and eventually succeed; the status page must count 503s.
3. MISS path -- a request with no fixture must surface an error, not
   silently return another page's data.

After the scenarios it polls /__status and asserts the aggregate
counters and that the last (empty) page interval was recorded with the
EMPTY flag. The proxy is left running for a short idle window so a human
can open/refresh the status page; use --no-idle in CI.

Usage::

    python scripts/offline_gate.py            # auto-manages a server
    python scripts/offline_gate.py --no-idle
"""

from __future__ import print_function

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
import urllib.error
import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from cryptocmd import CmcScraper  # noqa: E402
from cryptocmd.fetching import build_default_fetcher  # noqa: E402
from replay.build_fixtures import START, END, EMPTY_OVERFLOW_DAYS  # noqa: E402

HOST = os.environ.get("REPLAY_HOST", "127.0.0.1")
PORT = int(os.environ.get("REPLAY_PORT", "8877"))
BASE = "http://{}:{}".format(HOST, PORT)
FIXTURE_DIR = os.path.join(ROOT, "replay", "fixtures")


def _overflow_end():
    """Requested end date matching the recorded empty last page."""
    end = datetime.datetime.strptime(END, "%d-%m-%Y")
    return (end + datetime.timedelta(days=EMPTY_OVERFLOW_DAYS)).strftime("%d-%m-%Y")


def status_json():
    with urllib.request.urlopen(BASE + "/__status?format=json", timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def reset():
    req = urllib.request.Request(BASE + "/__reset", method="POST")
    with urllib.request.urlopen(req, timeout=5) as resp:
        resp.read()


def wait_for_server(timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status_json()
            return True
        except OSError:
            time.sleep(0.2)
    return False


def ensure_fixtures():
    index = os.path.join(FIXTURE_DIR, "index.json")
    if not os.path.exists(index):
        subprocess.check_call([sys.executable, os.path.join(ROOT, "replay", "build_fixtures.py")])


def start_server():
    log = open(os.path.join(ROOT, "replay", "server.log"), "ab")
    proc = subprocess.Popen(
        [sys.executable, "-m", "replay.server", "--host", HOST,
         "--port", str(PORT), "--fixture-dir", FIXTURE_DIR],
        cwd=ROOT, stdout=log, stderr=log,
    )
    if not wait_for_server():
        proc.terminate()
        raise RuntimeError("replay proxy failed to start")
    return proc


def scenario_hit():
    print("\n[1/3] HIT path: multi-page BTC download with empty last page")
    fetcher = build_default_fetcher(base_url=BASE, attempts=3)
    scraper = CmcScraper(
        "BTC", START, _overflow_end(), id_number=1,
        order_ascending=True, fetcher=fetcher,
    )
    _, rows = scraper.get_data()
    print("    -> OK, {} rows returned (empty last page merged cleanly)".format(len(rows)))
    return len(rows)


def scenario_retry():
    print("\n[2/3] RETRY path: flaky page served twice with HTTP 503")
    # Fresh fetcher -> fresh cache so the flaky page cannot be served
    # from the scenario-1 cache (cache would mask the retries).
    fetcher = build_default_fetcher(base_url=BASE, attempts=4)
    url = (
        BASE + "/data-api/v3/cryptocurrency/historical"
        "?id=1&convertId=2781&timeStart=1672444800&timeEnd=1703980800"
        "&__flaky=2"
    )
    response = fetcher(url)
    payload = response.json()
    assert payload["status"]["error_code"] == 0
    print("    -> OK, page delivered after retries")


def scenario_miss():
    print("\n[3/3] MISS path: unrecorded page must error, not return another page")
    fetcher = build_default_fetcher(base_url=BASE, attempts=1)
    url = (
        BASE + "/data-api/v3/cryptocurrency/historical"
        "?id=999999&convertId=2781&timeStart=1&timeEnd=2"
    )
    response = fetcher(url)
    assert response.status_code == 404, "expected 404 replay miss, got {}".format(
        response.status_code
    )
    body = response.json()
    print("    -> OK, 404 miss:", body["status"]["error_message"][:80])


def verify_state(row_count):
    snap = status_json()
    print("\nstatus page counters: {}".format(
        {k: snap[k] for k in ("total_requests", "hits", "misses", "server_errors")}
    ))

    errors = []

    # scenario 1 = listing + 3 pages = 4 hits. scenario 2 deliberately
    # uses a FRESH fetcher (its own cache, so retries aren't masked) and
    # re-requests page 1: that is one more successful hit on the SAME
    # key, hence the aggregate is still 4 while page 1's per-key hits=2.
    # The two flaky serves are counted separately as server_errors.
    if snap["hits"] != 4:
        errors.append("expected 4 aggregate 200-hits, got {}".format(snap["hits"]))
    if snap["misses"] != 1:
        errors.append("expected 1 miss, got {}".format(snap["misses"]))
    if snap["server_errors"] != 2:
        errors.append("expected 2 server-side retry triggers, got {}".format(
            snap["server_errors"]))

    historical = [k for k in snap["keys"]
                  if "historical" in k["key"] and k.get("interval")]
    last = [k for k in historical if k.get("empty") is True]
    if not last:
        errors.append("empty last page was not recorded as a distinct key")
    else:
        print("last/empty page interval:", last[0]["interval"])

    # page 1 must show its two 503s separately from its successful hits
    page1 = next((k for k in historical if k["retries"]), None)
    if page1 is None or page1["retries"] != 2:
        errors.append("flaky page did not record exactly 2 retries")

    if row_count != 730:
        errors.append("expected 730 merged rows (365+365+0), got {}".format(row_count))

    print("\nper-key page intervals:")
    for item in historical:
        flag = " [EMPTY LAST PAGE]" if item.get("empty") else ""
        print("    {} hits={} retries={}{}".format(
            item["interval"], item["hits"], item["retries"], flag))

    if errors:
        print("\nOFFLINE GATE FAILED:")
        for err in errors:
            print("  -", err)
        return False
    print("\nOFFLINE GATE PASSED: hit, miss and retry paths all exercised;")
    print("counters and the empty last-page interval match expectations.")
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description="cryptocmd offline gate")
    parser.add_argument("--no-idle", action="store_true",
                        help="exit immediately instead of leaving the server up")
    parser.add_argument("--idle", type=int, default=20,
                        help="seconds to leave the proxy up for manual checks")
    parser.add_argument("--external-server", action="store_true",
                        help="use an already-running proxy instead of spawning one")
    args = parser.parse_args(argv)

    ensure_fixtures()

    spawned = None
    if not args.external_server:
        spawned = start_server()

    try:
        reset()
        row_count = scenario_hit()
        scenario_retry()
        scenario_miss()
        ok = verify_state(row_count)
        if not ok:
            return 1

        if not args.no_idle:
            print("\nstatus page: {}/__status".format(BASE))
            print("leaving proxy up for {}s so you can open/refresh it...".format(
                args.idle))
            time.sleep(args.idle)
        return 0
    finally:
        if spawned is not None:
            spawned.terminate()
            try:
                spawned.wait(timeout=5)
            except subprocess.TimeoutExpired:
                spawned.kill()


if __name__ == "__main__":
    sys.exit(main())
