# -*- coding: utf-8 -*-
"""纯函数 / 缓存重试 / 并发翻页 / 回放服务 / fiat 校验 的单元测试。"""

import datetime
import json
import os
import subprocess
import sys
import threading

import pytest

from cryptocmd.utils import (
    FetchError,
    _CachingFetcher,
    build_historical_url,
    build_listing_url,
    download_coin_data,
    iter_page_windows,
    parse_date,
    parse_iso_open,
    request_key,
    slice_quotes,
    with_retry,
)

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ---------------------------------------------------------------------------
# 日期解析纯函数
# ---------------------------------------------------------------------------


class TestPureDates:
    def test_parse_date_single_digit(self):
        assert parse_date("28-4-2013") == datetime.date(2013, 4, 28)

    def test_parse_iso_open(self):
        assert parse_iso_open("2026-04-25T00:00:00.000Z") == datetime.date(2026, 4, 25)


# ---------------------------------------------------------------------------
# 分页切片纯函数：末页 / 空页 / 无缝
# ---------------------------------------------------------------------------


class TestPageWindows:
    def test_three_pages_full_empty_short(self):
        windows = list(
            iter_page_windows(parse_date("01-01-2026"), parse_date("12-01-2026"), 5)
        )
        assert windows == [
            (datetime.date(2025, 12, 31), datetime.date(2026, 1, 5)),
            (datetime.date(2026, 1, 5), datetime.date(2026, 1, 10)),
            (datetime.date(2026, 1, 10), datetime.date(2026, 1, 12)),
        ]

    def test_single_day_is_one_page_no_empty_tail(self):
        windows = list(
            iter_page_windows(parse_date("05-01-2026"), parse_date("05-01-2026"), 5)
        )
        assert windows == [(datetime.date(2026, 1, 4), datetime.date(2026, 1, 5))]

    def test_exact_multiple_has_no_trailing_empty_page(self):
        windows = list(
            iter_page_windows(parse_date("01-01-2026"), parse_date("10-01-2026"), 5)
        )
        assert len(windows) == 2
        assert windows[-1] == (datetime.date(2026, 1, 5), datetime.date(2026, 1, 10))

    def test_no_gaps_between_windows(self):
        windows = list(
            iter_page_windows(parse_date("01-01-2020"), parse_date("31-12-2024"), 30)
        )
        for prev, nxt in zip(windows, windows[1:]):
            assert prev[1] == nxt[0], "gap/overlap between page windows"

    def test_end_before_start_yields_nothing(self):
        assert (
            list(
                iter_page_windows(parse_date("10-01-2026"), parse_date("01-01-2026"), 5)
            )
            == []
        )


class TestSliceQuotes:
    @staticmethod
    def _q(day):
        return {"timeOpen": "2026-01-{:02d}T00:00:00.000Z".format(day)}

    def test_exclusive_start_inclusive_end(self):
        quotes = [self._q(d) for d in (4, 5, 6)]
        got = slice_quotes(
            quotes, datetime.date(2026, 1, 5), datetime.date(2026, 1, 10)
        )
        assert [q["timeOpen"][:10] for q in got] == ["2026-01-06"]


# ---------------------------------------------------------------------------
# 请求指纹：缓存键与夹具索引的共同口径
# ---------------------------------------------------------------------------


class TestRequestKey:
    def test_query_order_does_not_matter(self):
        a = "https://a/data-api/v3/x?id=1&timeStart=2&timeEnd=3"
        b = "http://127.0.0.1:9/data-api/v3/x?timeEnd=3&id=1&timeStart=2"
        assert request_key(a) == request_key(b)

    def test_different_page_windows_never_alias(self):
        base = "http://h/data-api/v3/cryptocurrency/historical"
        k1 = request_key(
            build_historical_url(
                base, 1, 2, datetime.date(2026, 1, 4), datetime.date(2026, 1, 5)
            )
        )
        k2 = request_key(
            build_historical_url(
                base, 1, 2, datetime.date(2026, 1, 5), datetime.date(2026, 1, 10)
            )
        )
        assert k1 != k2

    def test_listing_url_is_parseable(self):
        key = request_key(build_listing_url("http://127.0.0.1:1/data-api/v3"))
        assert key.startswith("/data-api/v3/cryptocurrency/listing?")


# ---------------------------------------------------------------------------
# 重试装饰器
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


class TestRetry:
    def test_retries_then_succeeds(self):
        calls = {"n": 0}

        def flaky(url):
            calls["n"] += 1
            if calls["n"] < 3:
                return _FakeResponse(503)
            return _FakeResponse(200, {"ok": True})

        wrapped = with_retry(flaky, attempts=3, backoff=0)
        assert wrapped("http://x").json() == {"ok": True}
        assert calls["n"] == 3

    def test_retries_exhausted_raises_fetcherror(self):
        wrapped = with_retry(lambda url: _FakeResponse(503), attempts=3, backoff=0)
        with pytest.raises(FetchError):
            wrapped("http://x")

    def test_4xx_is_not_retried(self):
        calls = {"n": 0}

        def client_error(url):
            calls["n"] += 1
            return _FakeResponse(404)

        wrapped = with_retry(client_error, attempts=3, backoff=0)
        assert wrapped("http://x").status_code == 404
        assert calls["n"] == 1


# ---------------------------------------------------------------------------
# 缓存装饰器：并发同键只打一次网络 + 深拷贝隔离
# ---------------------------------------------------------------------------


class TestCachingFetcher:
    def test_concurrent_same_key_single_network_call(self):
        counter = {"n": 0}

        def slow(url):
            counter["n"] += 1
            threading.Event().wait(0.2)
            return {"status": {"error_code": 0}, "data": {"quotes": []}}

        cached = _CachingFetcher(slow)
        results = []
        results_lock = threading.Lock()

        def worker():
            value = cached("http://x/a?b=1")
            with results_lock:
                results.append(value)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert counter["n"] == 1, "same page must be fetched once under concurrency"
        assert len(results) == 8

    def test_mutation_does_not_poison_cache(self):
        cached = _CachingFetcher(lambda url: {"data": {"quotes": [{"v": 1}]}})
        first = cached("http://x/a")
        first["data"]["quotes"].append({"v": 2})
        second = cached("http://x/a")
        assert second["data"]["quotes"] == [{"v": 1}]

    def test_error_is_not_cached(self):
        calls = {"n": 0}

        def boom_then_ok(url):
            calls["n"] += 1
            if calls["n"] == 1:
                raise FetchError("boom")
            return {"ok": True}

        cached = _CachingFetcher(boom_then_ok)
        with pytest.raises(FetchError):
            cached("http://x/a")
        assert cached("http://x/a") == {"ok": True}


# ---------------------------------------------------------------------------
# 并发翻页 + 注入 fetcher：每页拿到自己的窗口，不串页
# ---------------------------------------------------------------------------


class TestConcurrentPaging:
    def test_pages_do_not_get_cross_stored(self, monkeypatch):
        # 5 天一页 -> 满页 / 空页 / 短末页三页；fetcher 按请求自身的时间窗造
        # 数据，若并发下缓存把上一页存进下一页的键，拼出来的日期集合就会错。
        monkeypatch.setenv("CRYPTOCMD_PAGE_DAYS", "5")

        def listing(url):
            return {
                "status": {"error_code": 0},
                "data": {"cryptoCurrencyList": [{"id": 1, "symbol": "BTC"}]},
            }

        def historical(url):
            params = dict(p.split("=") for p in url.split("?")[1].split("&"))
            first = datetime.datetime.fromtimestamp(
                int(params["timeStart"]), datetime.timezone.utc
            ).date() + datetime.timedelta(days=1)
            last = datetime.datetime.fromtimestamp(
                int(params["timeEnd"]), datetime.timezone.utc
            ).date()
            quotes = []
            day = first
            while day <= last:
                if day != datetime.date(2026, 1, 8):  # 01-08 缺数据 -> 中间空页
                    quotes.append(
                        {
                            "timeOpen": day.strftime("%Y-%m-%dT00:00:00.000Z"),
                            "quote": {},
                        }
                    )
                day += datetime.timedelta(days=1)
            return {
                "status": {"error_code": 0, "error_message": None},
                "data": {"id": 1, "symbol": "BTC", "name": "Bitcoin", "quotes": quotes},
            }

        def raw_fetcher(url):
            return listing(url) if "listing" in url else historical(url)

        # 显式注入自带缓存/重试栈的 fetcher（与离线时构造方式一致）。
        fetcher = _CachingFetcher(with_retry(raw_fetcher, attempts=2, backoff=0))
        data = download_coin_data(
            "BTC",
            "01-01-2026",
            "12-01-2026",
            "USD",
            None,
            id_number=1,
            fetcher=fetcher,
            max_workers=4,
        )
        dates = sorted(
            parse_iso_open(q["timeOpen"]).isoformat() for q in data["data"]["quotes"]
        )
        expected = [
            datetime.date(2026, 1, d).isoformat()
            for d in (1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12)
        ]
        assert dates == expected


# ---------------------------------------------------------------------------
# 回放服务端到端（真实本地 HTTP，无外网依赖）
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def replay_server():
    sys.path.insert(0, os.path.join(ROOT, "scripts", "replay"))
    import urllib.request

    import server as replay_server_module

    httpd, _ = replay_server_module.start_server(0)
    port = httpd.server_address[1]
    yield port, urllib.request
    httpd.shutdown()


class TestReplayServer:
    def test_hit_and_miss_paths(self, replay_server):
        port, urllib = replay_server
        _perform_miss(port, urllib)
        good_url = (
            "http://127.0.0.1:{}/data-api/v3/cryptocurrency/historical"
            "?convertId=2781&id=9001&timeEnd=1767571200&timeStart=1767139200"
        ).format(port)
        with urllib.urlopen(good_url) as resp:
            assert resp.status == 200
            body = json.loads(resp.read())
            assert len(body["data"]["quotes"]) == 5

    def test_status_page_counts_and_last_interval(self, replay_server):
        port, urllib = replay_server
        _perform_miss(port, urllib)
        with urllib.urlopen("http://127.0.0.1:{}/__replay/state".format(port)) as resp:
            state = json.loads(resp.read())
        assert any(r["hits"] for r in state["rows"])
        assert any(r["misses"] for r in state["rows"])

        with urllib.urlopen("http://127.0.0.1:{}/__replay/status".format(port)) as resp:
            page = resp.read().decode("utf-8")
        assert 'http-equiv="refresh"' in page
        assert "HIT" in page and "MISS" in page


def _perform_miss(port, urllib):
    import urllib.error
    import urllib.request as _u

    miss_url = (
        "http://127.0.0.1:{}/data-api/v3/cryptocurrency/historical"
        "?convertId=2781&id=7777&timeEnd=1767571200&timeStart=1767139200"
    ).format(port)
    try:
        _u.urlopen(miss_url)
    except urllib.error.HTTPError as err:
        assert err.code == 503
    else:
        raise AssertionError("missing fixture should return 503")


# ---------------------------------------------------------------------------
# fiat_ids.json 独立校验脚本
# ---------------------------------------------------------------------------


class TestCheckFiatIds:
    def test_checker_passes_shipped_file(self):
        proc = subprocess.run(
            [
                sys.executable,
                os.path.join(ROOT, "scripts", "check_fiat_ids.py"),
                os.path.join(ROOT, "cryptocmd", "fiat_ids.json"),
            ],
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, proc.stderr

    def test_checker_catches_duplicate_values_and_bad_keys(self, tmp_path):
        bad = tmp_path / "fiat.json"
        bad.write_text(json.dumps({"USD": 1, "eur": 2, "ABC": 1}), encoding="utf-8")
        sys.path.insert(0, os.path.join(ROOT, "scripts"))
        import check_fiat_ids

        errors, _infos = check_fiat_ids.check(str(bad))
        assert any("eur" in e for e in errors)
        assert any("duplicate fiat id 1" in e for e in errors)

    def test_checker_catches_duplicate_json_keys(self, tmp_path):
        bad = tmp_path / "fiat.json"
        bad.write_text('{"USD": 1, "USD": 2}', encoding="utf-8")
        sys.path.insert(0, os.path.join(ROOT, "scripts"))
        import check_fiat_ids

        errors, _ = check_fiat_ids.check(str(bad))
        assert any("duplicate keys" in e for e in errors)
