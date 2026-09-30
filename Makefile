.PHONY: test test-unit check-fiat-ids replay-fixtures replay-server offline-gate

test: check-fiat-ids test-unit

test-unit:
	python -m pytest tests/ -m "not integration" -q

check-fiat-ids:
	python scripts/check_fiat_ids.py cryptocmd/fiat_ids.json

replay-fixtures:
	python scripts/replay/generate_fixtures.py

replay-server: replay-fixtures
	python scripts/replay/server.py --port 8765

# 拔网线后执行：所有外网连接会被门控自己的 socket 守卫拒绝，
# 只允许本机回放服务。未命中路径（FetchError + 重试）和命中路径都会输出，
# 结束后按打印的 URL 打开 /__replay/status 核对计数与末页区间。
offline-gate: replay-fixtures
	python scripts/replay/offline_gate.py
