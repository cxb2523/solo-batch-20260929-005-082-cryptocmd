#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Core plumbing for CmcScraper.

设计切分（三处互相牵制的点，改任何一处前先读这里）：

1. 回放命中率  <->  2. 并发缓存一致性  <->  3. 末页/空页边界

* 请求入口只有一个：``fetcher(url)``（见 :class:`Fetcher` 协议）。默认走真实
  CMC 接口；离线时 ``CRYPTOCMD_API_BASE`` 指向本地回放服务即可，业务代码不感知。
* 夹具/缓存统一按"请求指纹"索引：URL 路径 + 排序后的查询参数
  （:func:`request_key`）。缓存装饰器和回放服务必须共用同一份指纹口径，否则
  同一页换个参数顺序就会缓存 miss / 回放 miss。
* 分页切片是纯函数 :func:`iter_page_windows`，缓存键天然与切片口径对齐：每一页
  都有独立、确定的 (timeStart, timeEnd)，并发翻页时不同页绝不会算出同一个键。
* 末页规则：切片在 ``cursor == end`` 时立即停止，所以不会出现"紧贴 end 的空尾页"；
  短末页（剩余天数 < page_days）仍是一个独立页、独立键。
* 空页规则：中间空页（时间区间存在但无行情，如缺口数据）是合法的一页，下载循环
  不得因 quotes 为空就提前终止，否则末页区间会错、回放夹具也对不上。
* 缓存层包在重试层外面（cache(retry(网络))）：同一键的并发请求只放一个去打网络，
  其余线程等结果，杜绝"并发翻页重复命中、把上一页结果存成下一页"。
"""

from __future__ import print_function

import copy
import datetime
import json
import os
import sys
import threading
import time
from typing import Protocol

import requests

_CMC_DATA_API = "https://api.coinmarketcap.com/data-api/v3"

# 默认分页大小（天）。CMC 单次响应上限约 365 条日线。离线门控可通过
# CRYPTOCMD_PAGE_DAYS 调小，用小夹具覆盖"满页/空页/短末页"三种边界。
_DEFAULT_PAGE_DAYS = 365

_HEADERS = {"User-Agent": "Mozilla/5.0"}

with open(os.path.join(os.path.dirname(__file__), "fiat_ids.json")) as _f:
    _FIAT_IDS = json.load(_f)

_SUPPORTED_FIATS = ", ".join(sorted(_FIAT_IDS))


class InvalidParameters(ValueError):
    """Passed parameters are invalid."""


class InvalidCoinCode(NotImplementedError):
    """This coin code is unavailable on 'coinmarketcap.com'"""


class FetchError(RuntimeError):
    """fetcher 在重试耗尽后仍未拿到可用响应（含回放未命中）。"""


# ---------------------------------------------------------------------------
# 纯函数：日期解析
# ---------------------------------------------------------------------------


def parse_date(value):
    """dd-mm-yyyy 字符串 -> ``datetime.date``。纯函数，无副作用。"""
    return datetime.datetime.strptime(value, "%d-%m-%Y").date()


def parse_iso_open(time_open):
    """CMC 的 ``timeOpen`` (``%Y-%m-%dT%H:%M:%S.%fZ``) -> ``datetime.date``。"""
    return datetime.datetime.strptime(time_open, "%Y-%m-%dT%H:%M:%S.%fZ").date()


def to_utc_timestamp(dt_value):
    """naive ``datetime``/``date`` 按 UTC 转 epoch 秒。"""
    if isinstance(dt_value, datetime.date) and not isinstance(
        dt_value, datetime.datetime
    ):
        dt_value = datetime.datetime.combine(dt_value, datetime.time.min)
    return int(dt_value.replace(tzinfo=datetime.timezone.utc).timestamp())


# ---------------------------------------------------------------------------
# 纯函数：分页切片
# ---------------------------------------------------------------------------


def iter_page_windows(start, end, page_days):
    """把 [start, end] 闭区间切成互不重叠、不留缝的 (time_start, time_end) 页。

    返回的 time_start 是"排他"的：CMC 的 timeStart 不含当天（首个返回日 =
    timeStart + 1），因此第一页 time_start = start - 1d；后续页的 time_start
    直接接上一页的 time_end（cursor 不额外 +1，否则两页之间会漏掉一天）。

    边界口径（与回放夹具、缓存键三处共用）：
    * ``start == end``        -> 恰好一页（当天），不会产生空尾页；
    * 剩余天数 < page_days    -> 短末页，仍是独立一页、独立缓存键；
    * 某页行情为空            -> 调用方不得据此终止，后面的页仍然存在。
    """
    if page_days <= 0:
        raise ValueError("page_days must be positive")
    if end < start:
        return
    cursor = start - datetime.timedelta(days=1)
    while cursor < end:
        window_end = min(cursor + datetime.timedelta(days=page_days), end)
        yield cursor, window_end
        cursor = window_end  # cursor == end 时循环结束，不生成空尾页


def slice_quotes(quotes, time_start, time_end):
    """按一页的 (time_start, time_end] 口径从 quotes 中切出行情。

    夹具生成器和客户端拼页都用它，保证"本地切出来的一页"和"服务端按同一时间
    窗回放的一页"逐条对齐；缓存键 (timeStart, timeEnd) 与切片内容因此严格一致。
    """
    result = []
    for quote in quotes:
        day = parse_iso_open(quote["timeOpen"])
        if time_start < day <= time_end:
            result.append(quote)
    return result


# ---------------------------------------------------------------------------
# 纯函数：请求指纹（缓存与回放共用的唯一索引口径）
# ---------------------------------------------------------------------------


def request_key(url):
    """URL -> 稳定指纹：路径 + 按 key 排序后的查询参数。

    只认路径和查询串，不认 scheme/host/端口——同一条夹具在真实域名和
    127.0.0.1:PORT 下必须命中同一个键。参数顺序变化不会产生别名，这是
    "并发翻页不会把上一页存成下一页"的前提：timeStart/timeEnd 不同的页
    指纹必然不同，相同页（含参数乱序）指纹必然相同。
    """
    from urllib.parse import parse_qsl, urlsplit

    parts = urlsplit(url)
    query = sorted(parse_qsl(parts.query, keep_blank_values=True))
    return parts.path + "?" + "&".join("{}={}".format(k, v) for k, v in query)


def build_historical_url(api_base, coin_id, convert_id, time_start, time_end):
    """构造历史行情页 URL（time_* 为 date/datetime，内部统一转 UTC epoch）。"""
    return (
        "{}/cryptocurrency/historical?id={}&convertId={}&timeStart={}&timeEnd={}"
    ).format(
        api_base.rstrip("/"),
        coin_id,
        convert_id,
        to_utc_timestamp(time_start),
        to_utc_timestamp(time_end),
    )


def build_listing_url(api_base):
    """构造币种列表 URL（symbol -> coin id 查询）。"""
    return (
        "{}/cryptocurrency/listing?start=1&limit=5000"
        "&sortBy=market_cap&sortType=desc&convert=USD"
        "&cryptoType=all&tagType=all&audited=false"
    ).format(api_base.rstrip("/"))


# ---------------------------------------------------------------------------
# fetcher 协议 + 缓存/重试装饰器
# ---------------------------------------------------------------------------


class Fetcher(Protocol):
    """可注入的取数协议：给 URL，返回带 ``.json()`` 的响应（或 dict）。"""

    def __call__(self, url): ...


def _response_json(response):
    """兼容真实 requests.Response 与测试/回放直接返回 dict 的情况。"""
    if isinstance(response, dict):
        return response
    return response.json()


def with_retry(fetcher, attempts=3, backoff=0.2):
    """重试装饰器：仅对连接异常和 5xx（含回放未命中 503）重试。

    4xx 是确定性失败（如 fixture 明确不存在），不重试，原样透传给上层
    按状态码/响应体处理。
    """

    def wrapped(url):
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                response = fetcher(url)
                status = getattr(response, "status_code", 200)
                if 500 <= status < 600:
                    raise FetchError("HTTP {} for {}".format(status, request_key(url)))
                return response
            except (requests.RequestException, FetchError) as err:
                last_error = err
                if attempt < attempts:
                    time.sleep(backoff * attempt)
        raise FetchError(str(last_error))

    return wrapped


class _CachingFetcher:
    """缓存装饰器：键口径 = :func:`request_key`，与分页切片一一对应。

    并发语义：同一键只放行一个请求，其余线程等待共享结果。因此翻页并发时
    每个 (timeStart, timeEnd) 页只会被抓取一次，不存在重复命中后互相覆盖、
    把上一页响应写进下一页键的竞态。命中缓存时返回深拷贝，调用方修改响应
    （如合并 quotes 时写 result_json）不会污染缓存。
    """

    def __init__(self, fetcher):
        self._fetcher = fetcher
        self._store = {}
        self._inflight = {}
        self._lock = threading.Lock()

    def __call__(self, url):
        key = request_key(url)
        with self._lock:
            if key in self._store:
                return copy.deepcopy(self._store[key])
            pending = self._inflight.get(key)
            if pending is None:
                event = threading.Event()
                pending = {"event": event, "error": None, "value": None}
                self._inflight[key] = pending
                owner = True
            else:
                owner = False

        if not owner:
            pending["event"].wait()
            if pending["error"] is not None:
                raise FetchError(pending["error"])
            return copy.deepcopy(pending["value"])

        try:
            value = self._fetcher(url)
        except Exception as err:  # 唤醒所有等待者并透传同一个失败
            with self._lock:
                pending["error"] = str(err)
                self._inflight.pop(key, None)
            pending["event"].set()
            raise
        if not isinstance(value, dict):
            value = _response_json(value)
        with self._lock:
            self._store[key] = value
            self._inflight.pop(key, None)
        pending["event"].set()
        return copy.deepcopy(value)


def default_http_fetcher(url):
    """默认 fetcher：真实网络，requests.get。"""
    return requests.get(url, headers=_HEADERS, timeout=30)


def build_fetcher(fetcher=None):
    """组装 fetcher 栈：cache(retry(网络))。

    fetcher=None 时按环境变量决定网络落点：
    * CRYPTOCMD_API_BASE 仅替换 URL 里的 host（离线时指向回放服务）；
    * CRYPTOCMD_RETRIES / CRYPTOCMD_PAGE_DAYS 控制重试次数与分页大小。
    """
    if fetcher is not None:
        return _CachingFetcher(with_retry(fetcher))
    attempts = int(os.environ.get("CRYPTOCMD_RETRIES", "3"))
    return _CachingFetcher(with_retry(default_http_fetcher, attempts=attempts))


def api_base():
    """真实接口或回放服务的 base URL（由 CRYPTOCMD_API_BASE 注入）。"""
    return os.environ.get("CRYPTOCMD_API_BASE", _CMC_DATA_API)


def page_days():
    """分页大小（天），默认 365；离线门控可调小以覆盖各类边界。"""
    return int(os.environ.get("CRYPTOCMD_PAGE_DAYS", str(_DEFAULT_PAGE_DAYS)))


# ---------------------------------------------------------------------------
# 兼容层：历史名字 get_url_data 保留，旧测试 patch 它仍然生效
# ---------------------------------------------------------------------------

# 进程级默认 fetcher 栈（缓存因此能跨 download_coin_data 调用复用）。
_DEFAULT_FETCHER = build_fetcher()


def get_url_data(url, fetcher=None):
    """发请求的唯一入口。

    fetcher=None 时用进程级默认栈（真实网络 + 重试 + 缓存）；显式传入的
    fetcher 已自带缓存/重试栈，不再重复包裹。单参数形式保留给旧代码与
    现有测试（``patch("cryptocmd.utils.get_url_data")``）。
    """
    if fetcher is None:
        return _DEFAULT_FETCHER(url)
    return fetcher(url)


def get_coin_id(coin_code, coin_name, fetcher=None):
    """
    This method fetches the id of currency from the given code
    :param coin_code: coin code of a cryptocurrency e.g. btc
    :param coin_name: coin name in case of many coins with same code e.g. sol -> solana, solcoin
    :param fetcher: 可注入取数协议；None 用默认栈（真实接口/回放服务）
    :return: numeric coin id for the cryptocurrency on coinmarketcap.com
    """

    api_url = build_listing_url(api_base())

    try:
        response = (
            get_url_data(api_url, fetcher)
            if fetcher is not None
            else get_url_data(api_url)
        )
        json_data = _response_json(response)
        error_code = json_data["status"]["error_code"]
        if str(error_code) == "0":
            coins = json_data["data"]["cryptoCurrencyList"]
            matches = [
                c
                for c in coins
                if c["symbol"].upper() == coin_code.upper()
                and (coin_name is None or c["name"].lower() == coin_name.lower())
            ]
            if matches:
                return matches[0]["id"]
            raise InvalidCoinCode(
                "'{}' coin code is unavailable on coinmarketcap.com".format(coin_code)
            )
        if str(error_code) == "400":
            raise InvalidCoinCode(
                "'{}' coin code is unavailable on coinmarketcap.com".format(coin_code)
            )
        raise Exception(json_data["status"]["error_message"])
    except Exception as e:
        print("Error fetching coin id data for coin code {}".format(coin_code))
        print("Error message:", e)
        raise e


def download_coin_data(
    coin_code,
    start_date,
    end_date,
    fiat,
    coin_name,
    id_number=None,
    fetcher=None,
    max_workers=None,
):
    """
    Download price history for the specified cryptocurrency and time range.

    :param coin_code: coin code of a cryptocurrency e.g. btc
    :param start_date: date since when to scrape data (in the format of dd-mm-yyyy)
    :param end_date: date to which scrape the data (in dd-mm-yyyy). CMC only returns
        completed daily candles; the latest candle is the last completed UTC day.
    :param fiat: fiat code eg. USD, EUR
    :param coin_name: coin name in case of many coins with same code
    :param id_number: CMC cryptocurrency id; overrides coin_code/coin_name.
    :param fetcher: 可注入取数协议（None=默认栈）。离线时由 build_fetcher 指向
        本地回放服务；测试可直接注入假 fetcher。
    :param max_workers: 并发翻页线程数。None 时读 CRYPTOCMD_MAX_WORKERS，
        默认 1（顺序抓取，与历史行为一致，URL 顺序确定）。离线门控显式开
        并发。所有页窗口由纯函数 :func:`iter_page_windows` 预先算出，结果
        按窗口顺序回填——并发只改变抓取时机，不改变拼页顺序；每页缓存键
        独立，互不串写。
    :return: json data with historical OHLCV data for the cryptocurrency
    """

    if start_date is None:
        # default start date on coinmarketcap.com
        start_date = "28-4-2013"

    if end_date is None:
        end_date = datetime.date.today().strftime("%d-%m-%Y")

    coin_id = (
        id_number if id_number else get_coin_id(coin_code, coin_name, fetcher=fetcher)
    )
    convert_id = _FIAT_IDS.get(fiat.upper())
    if convert_id is None:
        raise ValueError(f"Unknown fiat '{fiat}'. Supported: {_SUPPORTED_FIATS}.")

    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)

    # 纯函数先把页窗口全部切好：这是"切片口径"的唯一事实来源，缓存键
    # (URL 里的 timeStart/timeEnd) 和回放夹具全部由它派生，天然对齐。
    windows = list(iter_page_windows(start_dt, end_dt, page_days()))
    urls = [
        build_historical_url(api_base(), coin_id, convert_id, win_start, win_end)
        for win_start, win_end in windows
    ]

    workers = (
        max_workers
        if max_workers is not None
        else int(os.environ.get("CRYPTOCMD_MAX_WORKERS", "1"))
    )
    try:
        if workers > 1 and len(urls) > 1:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=workers) as pool:
                if fetcher is None:
                    responses = list(pool.map(get_url_data, urls))
                else:
                    responses = list(
                        pool.map(lambda url: get_url_data(url, fetcher), urls)
                    )
        else:
            responses = [
                get_url_data(url, fetcher) if fetcher is not None else get_url_data(url)
                for url in urls
            ]

        all_quotes = []
        result_json = None
        # responses 已按 windows 顺序排列（线程池用 map 保序），短末页与
        # 中间空页都占位参与——空页 extend 空列表即可，绝不能跳过。
        for response in responses:
            json_data = _response_json(response)
            status = json_data.get("status", {})
            error_code = status.get("error_code")
            if error_code is not None and str(error_code) != "0":
                raise Exception(status.get("error_message", "Unknown error"))
            if result_json is None:
                result_json = json_data
            all_quotes.extend(json_data["data"]["quotes"])

        if result_json is None:
            # start > end 等极端情况：没有任何页，返回空结构而不是 KeyError。
            raise InvalidParameters("No page windows for the requested date range")

        result_json["data"]["quotes"] = all_quotes

        if id_number:
            show_coin_info = False
            if coin_code and coin_code.upper() != result_json["data"]["symbol"].upper():
                print(
                    f"INFO: Using 'id_number'! The 'coin_code' ({coin_code}) provided "
                    + "is different from the symbol returned."
                )
                show_coin_info = True
            if coin_name and coin_name.lower() != result_json["data"]["name"].lower():
                print(
                    f"INFO: Using 'id_number'! The 'coin_name' ({coin_name}) provided "
                    + "is different from the name returned."
                )
                show_coin_info = True
            if show_coin_info:
                print(
                    f"""The returned data belongs to coin "{result_json["data"]["name"]}", """
                    + f"""with symbol "{result_json["data"]["symbol"]}" """
                )
        return result_json
    except Exception as e:
        print(
            "Error fetching price data for {} for interval '{}' and '{}'".format(
                f"(id {id_number})" if id_number else coin_code,
                start_date,
                end_date,
            )
        )
        print("Error message (download_data) :", e)
        raise e


def _replace(s, bad_chars):
    if sys.version_info > (3, 0):
        # For Python 3
        without_bad_chars = str.maketrans("", "", bad_chars)
        return s.translate(without_bad_chars)
    else:
        # For Python 2
        import string

        identity = string.maketrans("", "")
        return s.translate(identity, bad_chars)
