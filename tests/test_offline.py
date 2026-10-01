"""
Tests for the offline/replay refactor:

* pure date/slicing helpers and the last/empty-page boundary;
* cache key alignment with slicing under concurrent pagination;
* retry decorator semantics;
* the replay proxy (hit / miss / flaky / status counters);
* the standalone fiat_ids.json validator.
"""

import datetime
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from unittest.mock import MagicMock

import pytest

from cryptocmd.fetching import (
    canonical_key,
    HttpFetcher,
    CachingFetcher,
    RetryingFetcher,
)
from cryptocmd.utils import (
    parse_date,
    iter_time_chunks,
    build_historical_url,
)


API = "http://example.test/data-api/v3"


# ---------------------------------------------------------------------------
# Pure slicing / key alignment
# ---------------------------------------------------------------------------


class TestSlicing:
    def test_chunks_cover_range_without_gap_or_overlap(self):
        start = parse_date("01-01-2023")
        end = parse_date("05-01-2025")
        pages = list(iter_time_chunks(start, end))

        # First page starts one day before the requested start
        # (CMC timeStart is exclusive).
        assert pages[0][0] == start - datetime.timedelta(days=1)
        assert pages[-1][1] == end

        for (_, prev_end), (next_start, _) in zip(pages, pages[1:]):
            # Previous timeEnd is itself exclusive: next timeStart ==
            # previous timeEnd => no overlap, no missing candle.
            assert next_start == prev_end

    def test_single_day_range_is_one_page(self):
        start = end = parse_date("10-06-2024")
        pages = list(iter_time_chunks(start, end))
        assert len(pages) == 1
        assert pages[0] == (start - datetime.timedelta(days=1), end)

    def test_every_page_has_distinct_key(self):
        """Cache/fixture identity is 1:1 with slices (the core invariant)."""
        start = parse_date("01-01-2023")
        end = parse_date("05-01-2025")
        urls = [
            build_historical_url(API, 1, 2781, ts, te)
            for ts, te in iter_time_chunks(start, end)
        ]
        keys = [canonical_key(u) for u in urls]
        assert len(set(keys)) == len(keys)

    def test_key_ignores_query_parameter_order(self):
        u1 = API + "/x?id=1&timeStart=10&timeEnd=20"
        u2 = API + "/x?timeEnd=20&id=1&timeStart=10"
        assert canonical_key(u1) == canonical_key(u2)

    def test_different_pages_never_share_a_key(self):
        u1 = API + "/x?id=1&timeStart=10&timeEnd=20"
        u2 = API + "/x?id=1&timeStart=21&timeEnd=30"
        assert canonical_key(u1) != canonical_key(u2)


# ---------------------------------------------------------------------------
# CachingFetcher — concurrency consistency
# ---------------------------------------------------------------------------


class TestCachingFetcher:
    def test_concurrent_pages_never_cross_contaminate(self):
        """Each page must always see its own body, even under contention."""
        urls = {
            build_historical_url(API, 1, 2781, ts, te): (ts, te)
            for ts, te in iter_time_chunks(parse_date("01-01-2020"),
                                           parse_date("01-01-2024"))
        }

        def transport(url, **kwargs):
            ts, te = urls[url]
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                "page": "{}-{}".format(int(ts.timestamp()), int(te.timestamp()))
            }
            return resp

        cache = CachingFetcher(HttpFetcher(base_url=API, transport=transport))
        url_list = list(urls)

        # Hammer the same page set from many threads simultaneously.
        def worker(seed):
            for u in url_list:
                body = cache(u).json()
                ts, te = urls[u]
                expected = "{}-{}".format(int(ts.timestamp()), int(te.timestamp()))
                assert body["page"] == expected, "page body landed on the wrong key"

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(20)))

        # Exactly one underlying fetch per page.
        assert cache.misses == len(url_list)
        assert cache.hits == len(url_list) * 19

    def test_error_responses_are_not_cached(self):
        calls = {"n": 0}

        def transport(url, **kwargs):
            calls["n"] += 1
            resp = MagicMock()
            resp.status_code = 500
            return resp

        cache = CachingFetcher(HttpFetcher(base_url=API, transport=transport))
        cache(API + "/x")
        cache(API + "/x")
        assert calls["n"] == 2  # error page must not poison the cache

    def test_single_flight_same_key_one_origin_call(self):
        """N threads racing on one cold page produce exactly 1 fetch."""
        import time

        origin_calls = {"n": 0}
        gate = threading.Event()

        def transport(url, **kwargs):
            origin_calls["n"] += 1
            gate.wait(timeout=2)  # hold the leader until all waiters queue
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"ok": True}
            return resp

        cache = CachingFetcher(HttpFetcher(base_url=API, transport=transport))
        url = API + "/x?id=1&timeStart=10&timeEnd=20"
        results = []

        def worker():
            results.append(cache(url).json())

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(worker) for _ in range(8)]
            # Give the leader time to enter the origin call, then release.
            for f in futures:
                pass
            gate.set()
            [f.result() for f in futures]

        assert origin_calls["n"] == 1
        assert cache.misses == 1
        assert cache.hits == 7
        assert all(r == {"ok": True} for r in results)


# ---------------------------------------------------------------------------
# RetryingFetcher
# ---------------------------------------------------------------------------


class TestRetryingFetcher:
    def test_retries_5xx_then_succeeds(self):
        attempts = []

        def inner(url):
            attempts.append(True)
            resp = MagicMock()
            resp.status_code = 503 if len(attempts) < 3 else 200
            return resp

        out = RetryingFetcher(inner, attempts=3, backoff=0)("u")
        assert out.status_code == 200
        assert len(attempts) == 3

    def test_4xx_is_not_retried(self):
        attempts = []

        def inner(url):
            attempts.append(True)
            resp = MagicMock()
            resp.status_code = 404
            return resp

        out = RetryingFetcher(inner, attempts=5, backoff=0)("u")
        assert out.status_code == 404
        assert len(attempts) == 1

    def test_connection_error_is_retried(self):
        import requests

        attempts = {"n": 0}

        def inner(url):
            attempts["n"] += 1
            if attempts["n"] < 2:
                raise requests.ConnectionError("boom")
            resp = MagicMock()
            resp.status_code = 200
            return resp

        assert RetryingFetcher(inner, attempts=3, backoff=0)("u").status_code == 200


# ---------------------------------------------------------------------------
# Replay proxy end to end
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def replay_server(tmp_path_factory):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from replay import server as replay_mod
    from replay.build_fixtures import FIXTURE_DIR, main as build

    build()

    handler = replay_mod.ReplayHandler
    handler.fixture_dir = FIXTURE_DIR
    handler._index = None
    handler._flaky_seen = {}
    handler.state = replay_mod.ReplayState()

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield "http://127.0.0.1:{}".format(httpd.server_address[1])
    httpd.shutdown()


class TestReplayProxy:
    def test_hit_and_empty_last_page_and_miss(self, replay_server):
        import urllib.request
        import urllib.error

        base = replay_server
        # empty last page fixture exists (page 3 of the generated range)
        with urllib.request.urlopen(
            base + "/data-api/v3/cryptocurrency/listing"
            "?start=1&limit=5000&sortBy=market_cap&sortType=desc"
            "&convert=USD&cryptoType=all&tagType=all&audited=false"
        ) as r:
            assert r.status == 200
            assert json.loads(r.read())["status"]["error_code"] == 0

        try:
            urllib.request.urlopen(
                base + "/data-api/v3/cryptocurrency/historical"
                "?id=42&convertId=2781&timeStart=1&timeEnd=2"
            )
            assert False, "expected 404"
        except urllib.error.HTTPError as e:
            assert e.code == 404

    def test_flaky_page_fails_then_serves(self, replay_server):
        import urllib.request
        import urllib.error

        base = replay_server
        url = (
            base + "/data-api/v3/cryptocurrency/historical"
            "?id=1&convertId=2781&timeStart=1672444800&timeEnd=1703980800"
            "&__flaky=1"
        )
        try:
            urllib.request.urlopen(url)
            assert False, "first serve should be 503"
        except urllib.error.HTTPError as e:
            assert e.code == 503
        with urllib.request.urlopen(url) as r:
            assert r.status == 200


# ---------------------------------------------------------------------------
# fiat validator
# ---------------------------------------------------------------------------


class TestCheckFiat:
    def test_validator_detects_bad_structure_and_duplicates(self, tmp_path):
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__))), "scripts"))
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "check_fiat_ids",
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "scripts", "check_fiat_ids.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps({"usd": 1, "EUR": 2790, "GBP": 2790, "X": -3}))
        errors = mod.validate(str(bad))
        joined = " | ".join(errors)
        assert "invalid fiat code" in joined
        assert "duplicate CMC id 2790" in joined
        assert "positive integer" in joined

        good = tmp_path / "good.json"
        good.write_text(json.dumps({"USD": 2781, "EUR": 2790}))
        assert mod.validate(str(good)) == []
