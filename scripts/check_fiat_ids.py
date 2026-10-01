#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Standalone validator for ``cryptocmd/fiat_ids.json``.

This is deliberately independent of the scraper package:

* the JSON must be a flat ``{fiat_code: coinmarketcap_id}`` object -- no
  nesting, no extra wrappers, because ``utils.download_coin_data`` does
  a direct ``_FIAT_IDS[fiat]`` lookup;
* keys are uppercased 3/4-letter currency codes;
* values are positive integers;
* two fiat codes must never map to the same CMC id (a duplicated id
  would silently download the wrong fiat's history);
* the same id appearing under two codes, or the same file colliding
  with itself, is reported with every offending location.

Exit code 0 = valid, 1 = invalid (details on stdout).

Usage::

    python scripts/check_fiat_ids.py [path/to/fiat_ids.json]
"""

from __future__ import print_function

import json
import re
import sys
from collections import defaultdict

DEFAULT_PATH = "cryptocmd/fiat_ids.json"
_CODE_RE = re.compile(r"^[A-Z]{3,4}$")


def validate(path):
    errors = []

    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        return ["cannot load {}: {}".format(path, exc)]

    if not isinstance(data, dict):
        return ["top-level JSON value must be an object mapping CODE to id, got {}".format(
            type(data).__name__
        )]

    ids_to_codes = defaultdict(list)

    for code, value in data.items():
        if not isinstance(code, str) or not _CODE_RE.match(code):
            errors.append("invalid fiat code: {!r} (need 3-4 uppercase letters)".format(code))
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            errors.append("id for {!r} must be a positive integer, got {!r}".format(code, value))
        else:
            ids_to_codes[value].append(code)

    for fiat_id, codes in sorted(ids_to_codes.items()):
        if len(codes) > 1:
            errors.append(
                "duplicate CMC id {} mapped from multiple fiats: {}".format(
                    fiat_id, ", ".join(sorted(codes))
                )
            )

    if not data:
        errors.append("fiat map is empty")

    return errors


def main(argv):
    path = argv[1] if len(argv) > 1 else DEFAULT_PATH
    errors = validate(path)
    if errors:
        print("FAIL: {} validation error(s) in {}".format(len(errors), path))
        for err in errors:
            print("  -", err)
        return 1
    print("OK: {} is valid".format(path))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
