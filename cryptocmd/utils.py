#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

__all__ = [
    "download_coin_data",
    "get_coin_id",
    "get_url_data",
    "parse_date",
    "parse_quote_date",
    "default_end_date",
    "iter_time_chunks",
    "build_historical_url",
    "build_listing_url",
    "InvalidParameters",
    "InvalidCoinCode",
]

import json
import os
import sys
import datetime
from concurrent.futures import ThreadPoolExecutor
from requests import get

from .fetching import build_default_fetcher


_CMC_DATA_API = "https://api.coinmarketcap.com/data-api/v3"
_PAGE_LIMIT_DAYS = 365

with open(os.path.join(os.path.dirname(__file__), "fiat_ids.json")) as _f:
    _FIAT_IDS = json.load(_f)

_SUPPORTED_FIATS = ", ".join(sorted(_FIAT_IDS))

_HEADERS = {"User-Agent": "Mozilla/5.0"}


# ---------------------------------------------------------------------------
# Pure helpers (no I/O)
#
# These are deliberately side-effect free so that the *slicing* policy and
# the cache/key policy can be tested and reasoned about independently:
#   - iter_time_chunks determines which pages exist;
#   - fetching.canonical_key determines how a page (URL) is keyed.
# Both operate on the same timeStart/timeTimeEnd boundaries, which is what
# keeps replay lookup, cache identity and the empty/last-page rule aligned.
# ---------------------------------------------------------------------------


def parse_date(value):
    """Parse a ``dd-mm-yyyy`` date string into an aware UTC datetime."""
    return datetime.datetime.strptime(value, "%d-%m-%Y").replace(
        tzinfo=datetime.timezone.utc
    )


def parse_quote_date(time_open):
    """Convert a quote ``timeOpen`` ISO timestamp to ``dd-mm-yyyy`` (pure)."""
    return datetime.datetime.strptime(
        time_open, "%Y-%m-%dT%H:%M:%S.%fZ"
    ).strftime("%d-%m-%Y")


def default_end_date(today=None):
    """Default end date (today, UTC) as ``dd-mm-yyyy``.

    CMC only returns completed daily candles, but the requested upper
    bound is still today, not yesterday.
    """
    today = today or datetime.date.today()
    return today.strftime("%d-%m-%Y")


def iter_time_chunks(start_dt, end_dt, page_limit_days=_PAGE_LIMIT_DAYS):
    """
    Slice ``[start_dt, end_dt]`` into requests of at most
    ``page_limit_days`` days. Yields ``(time_start, time_end)`` pairs.

    The pairs are the *actual URL parameters* for each page:
      * ``timeStart`` is exclusive on CMC (first candle = timeStart + 1),
        so a page starts one day before the first wanted day;
      * the next page's ``timeStart`` equals the previous ``timeEnd``
        (which is already exclusive), so slices never overlap and never
        leave a gap -- advancing two days instead would drop exactly
        one candle at every page boundary.

    Because these exact two values are what differentiate the URLs
    passed to the fetcher, ``fetching.canonical_key(url)`` and this
    slicer share one granularity: every yielded page is a distinct key
    and every key maps to exactly one page. The final yielded page may
    legitimately carry zero quotes (delisted coin / future range) --
    callers must treat an empty last page as a valid page, not an error.
    """
    chunk_start = start_dt - datetime.timedelta(days=1)
    while chunk_start < end_dt:
        chunk_end = min(
            chunk_start + datetime.timedelta(days=page_limit_days), end_dt
        )
        yield chunk_start, chunk_end
        chunk_start = chunk_end


def build_historical_url(api_base, coin_id, convert_id, time_start, time_end):
    """Build one historical-page URL from a slice produced by
    :func:`iter_time_chunks`. Keeping URL construction here (next to the
    slicer) ensures cache/key granularity can never drift from slicing."""
    return (
        "{}/cryptocurrency/historical"
        "?id={}&convertId={}&timeStart={}&timeEnd={}"
    ).format(
        api_base.rstrip("/"),
        coin_id,
        convert_id,
        int(time_start.timestamp()),
        int(time_end.timestamp()),
    )


def build_listing_url(api_base):
    """Build the coin-listing URL used to resolve a coin code to an id."""
    return (
        "{}/cryptocurrency/listing?start=1&limit=5000"
        "&sortBy=market_cap&sortType=desc&convert=USD"
        "&cryptoType=all&tagType=all&audited=false"
    ).format(api_base.rstrip("/"))


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


def get_url_data(url):
    """
    Backwards-compatible one-shot GET helper.

    New code should go through an injected fetcher (see
    :class:`cryptocmd.fetching.Fetcher`) so the transport can be pointed
    at the replay proxy. This thin wrapper keeps the historical import
    surface working and remains patch target compatible
    (``cryptocmd.utils.get_url_data``).
    """
    try:
        return get(url, headers=_HEADERS)
    except Exception as e:
        print("Error message (get_url_data) :", e)
        raise e


def _json(fetcher, url):
    return fetcher(url).json()


def get_coin_id(coin_code, coin_name, fetcher=None):
    """
    This method fetches the id of currency from the given code
    :param coin_code: coin code of a cryptocurrency e.g. btc
    :param coin_name: coin name in case of many coins with same code e.g. sol -> solana, solcoin
    :param fetcher: (optional) injectable fetcher; defaults to the live API.
    :return: numeric coin id for the cryptocurrency on coinmarketcap.com
    """

    fetcher = fetcher or build_default_fetcher()
    api_url = build_listing_url(_CMC_DATA_API)

    try:
        json_data = _json(fetcher, api_url)
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
    max_workers=4,
):
    """
    Download price history for the specified cryptocurrency and time range from CoinMarketCap.

    :param coin_code: coin code of a cryptocurrency e.g. btc
    :param start_date: date since when to scrape data (in the format of dd-mm-yyyy)
    :param end_date: date to which scrape the data (in the format of dd-mm-yyyy).
        Defaults to today. CMC only returns completed daily candles, so the latest
        candle returned will always be the last completed UTC day regardless of this value.
    :param fiat: fiat code eg. USD, EUR
    :param coin_name: coin name in case of many coins with same code e.g. sol -> solana, solcoin
    :param id_number: id number for the token on coinmarketcap. Will override coin_code and coin_name when provided.
    :param fetcher: injectable fetcher (live API by default; pass a fetcher
        whose base URL is the local replay proxy for offline runs).
    :param max_workers: page-fetch concurrency. Pages are merged back in
        slice order regardless of completion order; the per-page cache key
        (timeStart/timeEnd) prevents cross-page contamination.

    :return: json data with historical OHLCV data for the cryptocurrency
    """

    if start_date is None:
        # default start date on coinmarketcap.com
        start_date = "28-4-2013"

    if end_date is None:
        end_date = default_end_date()

    # A single fetcher stack (retry -> cache -> transport) is shared by
    # the listing lookup and every page, so concurrent pages share one
    # consistent cache.
    fetcher = fetcher or build_default_fetcher()

    coin_id = id_number if id_number else get_coin_id(
        coin_code, coin_name, fetcher=fetcher
    )
    convert_id = _FIAT_IDS.get(fiat.upper())
    if convert_id is None:
        raise ValueError(f"Unknown fiat '{fiat}'. Supported: {_SUPPORTED_FIATS}.")

    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)

    # Pages = slices from the one slicing function. Their URLs, and thus
    # their cache/fixture keys, are derived from the exact same
    # (timeStart, timeEnd) pairs -- see iter_time_chunks docstring for
    # the replay/cache/empty-page coupling.
    pages = list(iter_time_chunks(start_dt, end_dt))
    page_urls = [
        build_historical_url(_CMC_DATA_API, coin_id, convert_id, ts, te)
        for ts, te in pages
    ]

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            # Preserve submission (slice) order; concurrency must not
            # reorder pages or let one page's body land on another key.
            page_payloads = list(pool.map(lambda url: _json(fetcher, url), page_urls))

        all_quotes = []
        result_json = None
        for json_data in page_payloads:
            status = json_data.get("status", {})
            error_code = status.get("error_code")
            if error_code and str(error_code) != "0":
                raise Exception(status.get("error_message", "Unknown error"))
            if result_json is None:
                result_json = json_data
            all_quotes.extend(json_data["data"]["quotes"])

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
                    f"""The returned data belongs to coin "{result_json['data']['name']}", """
                    + f"""with symbol "{result_json['data']['symbol']}" """
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


class InvalidParameters(ValueError):
    """Passed parameters are invalid."""


class InvalidCoinCode(NotImplementedError):
    """This coin code is unavailable on 'coinmarketcap.com'"""
