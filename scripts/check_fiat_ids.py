#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""fiat_ids.json 独立校验：结构 + 重复键/重复值。

刻意不 import cryptocmd 包——校验是包之外的独立关卡（make check-fiat-ids），
即使包代码本身坏了，这份检查仍应能运行。

检查项：
1. 文件是合法 JSON，且顶层必须是对象（dict）；
2. 重复键：JSON 规范允许重复键但会静默覆盖，这里用 object_pairs_hook 抓出来；
3. 每个 key 必须是 3 位大写字母币种代码（^[A-Z]{3}$）；
4. 每个 value 必须是正整数（CMC 的 fiat id）；
5. 不同 key 不允许指向同一个 id（重复值）。

排序问题只报 INFO 不致命（不影响取数）。任何错误都以退出码 1 结束，
方便 CI / Makefile 关卡拦挡。
"""

from __future__ import print_function

import argparse
from collections import Counter, defaultdict
import json
import re
import sys

_KEY_RE = re.compile(r"^[A-Z]{3}$")


def load_with_duplicate_keys(path):
    """返回 (data, duplicate_keys)；重复键在 JSON 解析阶段就被抓到。"""
    duplicates = []

    def hook(pairs):
        seen = Counter(k for k, _ in pairs)
        duplicates.extend([k for k, count in seen.items() if count > 1])
        return dict(pairs)

    with open(path, encoding="utf-8") as handle:
        return json.load(handle, object_pairs_hook=hook), sorted(set(duplicates))


def check(path):
    errors = []
    infos = []

    try:
        data, duplicate_keys = load_with_duplicate_keys(path)
    except ValueError as err:
        return ["{}: invalid JSON: {}".format(path, err)], infos
    except OSError as err:
        return ["{}: cannot open file: {}".format(path, err)], infos

    if duplicate_keys:
        errors.append(
            "duplicate keys found (later entries silently overwrite earlier "
            "ones): {}".format(", ".join(duplicate_keys))
        )

    if not isinstance(data, dict):
        return errors + [
            "top-level JSON value must be an object mapping fiat code -> CMC numeric id"
        ], infos

    for code, fiat_id in data.items():
        if not isinstance(code, str) or not _KEY_RE.match(code):
            errors.append(
                "key %r: fiat code must be 3 uppercase letters, e.g. USD" % (code,)
            )
        if isinstance(fiat_id, bool) or not isinstance(fiat_id, int) or fiat_id <= 0:
            errors.append(
                "value for %r: %r is not a positive integer CMC fiat id"
                % (code, fiat_id)
            )

    ids_to_codes = defaultdict(list)
    for code, fiat_id in data.items():
        if isinstance(fiat_id, int) and not isinstance(fiat_id, bool):
            ids_to_codes[fiat_id].append(code)
    for fiat_id, codes in sorted(ids_to_codes.items()):
        if len(codes) > 1:
            errors.append(
                "duplicate fiat id %d mapped from multiple codes: %s"
                % (fiat_id, ", ".join(sorted(codes)))
            )

    keys = list(data.keys())
    if keys != sorted(keys):
        infos.append("keys are not sorted alphabetically (cosmetic only)")

    return errors, infos


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "path",
        nargs="?",
        default="cryptocmd/fiat_ids.json",
        help="path to fiat_ids.json (default: cryptocmd/fiat_ids.json)",
    )
    args = parser.parse_args(argv)

    errors, infos = check(args.path)
    for message in infos:
        print("INFO: {}".format(message))
    if errors:
        for message in errors:
            print("ERROR: {}".format(message), file=sys.stderr)
        print(
            "FAIL: {} problem(s) in {}".format(len(errors), args.path),
            file=sys.stderr,
        )
        return 1
    print("OK: {} passed structure and duplicate checks".format(args.path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
