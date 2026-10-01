#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Injectable HTTP fetch layer for the CoinMarketCap scraper.

Design contract (the three moving parts that must stay in sync)
=================================================================

1. **Replay hit rate** -- the replay proxy indexes fixtures by the *full*
   request identity: URL path plus the *canonicalised* query string
   (see :func:`canonical_key`). Every page of historical data differs by
   ``timeStart``/``timeEnd``, so two different pages can never alias the
   same fixture entry.

2. **Cache consistency under concurrent pagination** -- the cache layer
   keys on exactly the same identity (``canonical_key(url)``), i.e. the
   same granularity at which :func:`cryptocmd.utils.iter_time_chunks`
   slices the date range. The cache key therefore *is* the page key.
   If slicing ever changed (e.g. overlapping chunks) without changing
   the cache key, two concurrent workers paging the same range could
   resolve to the same key and one would persist the previous page's
   body as the next page. Keeping one identity function for both fixture
   lookup and caching makes that class of bug unrepresentable.

3. **Last/empty page boundary** -- neither the replay proxy nor the
   cache treats an empty ``quotes`` list specially: an empty page is a
   normal, independently keyed response. Only ``2xx`` responses are
   cached, so a retried/error last page can never poison a cache entry.

Composition order (outermost first)::

    RetryingFetcher(CachingFetcher(HttpFetcher(base_url)))

The retry wrapper is deliberately *outside* the cache: a transient
failure must be retried against the wire (or proxy) and only the final
successful response is memoised. The other order would cache failures
and swallow retries.
"""

from __future__ import print_function

__all__ = [
    "Fetcher",
    "HttpFetcher",
    "RetryingFetcher",
    "CachingFetcher",
    "canonical_key",
    "request_identity",
    "build_default_fetcher",
]

import os
import time
import threading
from urllib.parse import urlsplit, parse_qsl, urlunsplit

import requests


_DEFAULT_API_BASE = "https://api.coinmarketcap.com/data-api/v3"
_HEADERS = {"User-Agent": "Mozilla/5.0"}


def canonical_key(url, include_host=True):
    """
    Return the canonical identity used both for fixture lookup (replay
    proxy) and for cache entries.

    Identity = URL path + query parameters sorted by name. The fragment
    and query parameter ordering are ignored. By default the host is
    included so different services can share one cache; pass
    ``include_host=False`` for host-independent identity (the replay
    proxy uses that so a fixture hits regardless of the local port it
    is served on).

    This function is the single source of truth shared by the slicing
    logic, the cache and the replay proxy -- see the module docstring
    for why they must never diverge.
    """
    parts = urlsplit(url)
    query = tuple(sorted((k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)))
    netloc = parts.netloc if include_host else ""
    scheme = parts.scheme if include_host else ""
    return urlunsplit((scheme, netloc, parts.path, _encode_query(query), ""))


def request_identity(url):
    """Host-independent page identity (path + canonical query).

    Shared by the replay proxy and fixture generation; it is strictly
    coarser than :func:`canonical_key` only in the host component, so
    the page/slice granularity -- and the cache consistency argument in
    the module docstring -- is unchanged.
    """
    return canonical_key(url, include_host=False)


def _encode_query(pairs):
    from urllib.parse import urlencode

    return urlencode(list(pairs))


class Fetcher(object):
    """
    Fetcher protocol. Any object implementing ``fetch(url)`` (or this
    callable interface) can be injected into the scraper::

        scraper = CmcScraper("BTC", fetcher=my_fetcher)

    The default implementation talks to the live API; pointing
    ``base_url`` at the local replay proxy (see replay/server.py) makes
    the scraper fully offline.
    """

    def fetch(self, url):
        raise NotImplementedError

    def __call__(self, url):
        return self.fetch(url)


class HttpFetcher(Fetcher):
    """Fetcher that performs real HTTP GET requests via ``requests``."""

    def __init__(self, base_url=_DEFAULT_API_BASE, transport=None):
        self.base_url = base_url.rstrip("/")
        # ``transport`` is injectable for tests; defaults to requests.get.
        self._transport = transport or requests.get

    def rewrite(self, url):
        """
        Rewrite an absolute API URL onto this fetcher's base URL,
        preserving path and query. This is how the *same* code path
        reaches either the live API or the local replay proxy.
        """
        parts = urlsplit(url)
        if self.base_url and parts.netloc:
            base = urlsplit(self.base_url)
            return urlunsplit((base.scheme, base.netloc, parts.path, parts.query, ""))
        return url

    def fetch(self, url):
        return self._transport(self.rewrite(url), headers=_HEADERS)


class RetryingFetcher(Fetcher):
    """
    Retry decorator for a fetcher.

    Retries connection errors and ``>= 500`` responses (which is also
    how the replay proxy signals a deliberately flaky fixture). Client
    errors (``4xx``) are returned immediately -- retrying a missing
    fixture would just hide the miss.
    """

    def __init__(self, inner, attempts=3, backoff=0.05, sleep=time.sleep):
        self.inner = inner
        self.attempts = max(1, attempts)
        self.backoff = backoff
        self._sleep = sleep

    def fetch(self, url):
        last_exc = None
        for attempt in range(self.attempts):
            try:
                response = self.inner(url)
            except requests.RequestException as exc:
                last_exc = exc
            else:
                status = getattr(response, "status_code", None)
                if status is None or int(status) < 500:
                    return response
                last_exc = requests.HTTPError(
                    "upstream status {}".format(status), response=response
                )
            if attempt < self.attempts - 1:
                self._sleep(self.backoff * (2 ** attempt))
        raise last_exc


class CachingFetcher(Fetcher):
    """
    Thread-safe response cache decorator.

    The key is :func:`canonical_key` -- the *exact* page identity used
    by ``iter_time_chunks``. Concurrent pagination workers therefore
    get a 1:1 mapping of page -> cache entry; there is no coarser key
    under which a later page could be stored with an earlier page's
    body. Only successful (``2xx``) responses are cached, and the
    returned object is shared intentionally (responses are read once
    and never mutated).

    A per-key in-flight event makes the first request for a page a
    single-flight: concurrent workers for the *same* page wait for the
    origin response instead of each issuing their own and racing to
    store. Distinct pages (distinct keys) remain fully parallel. This
    is the property that makes the cache counters line up exactly with
    the number of pages requested under concurrent pagination.
    """

    def __init__(self, inner):
        self.inner = inner
        self._store = {}
        self._inflight = {}
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def fetch(self, url):
        key = canonical_key(url)
        while True:
            with self._lock:
                cached = self._store.get(key)
                if cached is not None:
                    self.hits += 1
                    return cached
                event = self._inflight.get(key)
                if event is None:
                    event = threading.Event()
                    self._inflight[key] = event
                    leader = True
                else:
                    leader = False

            if not leader:
                # Same page is in flight: wait, then re-check the cache.
                event.wait()
                continue  # cached -> hit; leader failure -> retry as leader

            try:
                response = self.inner(url)
                status = getattr(response, "status_code", None)
                if status is not None and not (200 <= int(status) < 300):
                    # 4xx = definitive miss (e.g. replay 404). 5xx is a
                    # transient failure owned by the outer retry wrapper:
                    # neither hit nor miss, and never cached -- caching it
                    # would swallow retries and could poison a page.
                    if status is not None and int(status) < 500:
                        with self._lock:
                            self.misses += 1
                    return response
                with self._lock:
                    self.misses += 1
                    self._store[key] = response
                return response
            finally:
                with self._lock:
                    self._inflight.pop(key, None)
                event.set()


def build_default_fetcher(base_url=None, attempts=3, cache=True):
    """
    Construct the default fetcher stack.

    :param base_url: override the API base URL. Defaults to the live
        CoinMarketCap API, or ``$CRYPTOCMD_API_BASE`` when set (the
        offline gate points this at the local replay proxy).
    :param attempts: total attempts made by the retry wrapper.
    :param cache: wrap the transport in a shared response cache.
    """
    base_url = base_url or os.environ.get("CRYPTOCMD_API_BASE") or _DEFAULT_API_BASE
    fetcher = HttpFetcher(base_url)
    if cache:
        fetcher = CachingFetcher(fetcher)
    return RetryingFetcher(fetcher, attempts=attempts)
