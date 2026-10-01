.PHONY: help fixtures replay-server offline-gate check-fiat test clean

PYTHON ?= python
REPLAY_HOST ?= 127.0.0.1
REPLAY_PORT ?= 8877

help:
	@echo "targets:"
	@echo "  make fixtures        generate deterministic replay fixtures"
	@echo "  make replay-server   run the local replay proxy (status page at /__status)"
	@echo "  make offline-gate    unplug-network gate: hit/miss/retry paths + state checks"
	@echo "  make check-fiat      validate cryptocmd/fiat_ids.json structure/duplicates"
	@echo "  make test            run unit tests"

fixtures:
	$(PYTHON) replay/build_fixtures.py

replay-server:
	$(PYTHON) -m replay.server --host $(REPLAY_HOST) --port $(REPLAY_PORT)

# Intended to be run with the network unplugged: it spawns the local
# replay proxy, drives the hit, miss and retry scenarios, verifies the
# status-page counters and the empty last-page interval, then leaves
# the proxy up briefly so the dashboard can be opened/refreshed.
offline-gate: fixtures
	CRYPTOCMD_API_BASE=http://$(REPLAY_HOST):$(REPLAY_PORT) \
	$(PYTHON) scripts/offline_gate.py

check-fiat:
	$(PYTHON) scripts/check_fiat_ids.py

test:
	$(PYTHON) -m pytest tests -m "not integration" -q

clean:
	rm -rf replay/fixtures .pytest_cache
