# GEKTOR APEX v3.6.0 — Operator Makefile
#
# Quick reference for local development. Production deployment uses
# systemd on the Tokyo VPS (out of scope for this Makefile).

.PHONY: help install test test-radar test-vpin test-pipeline lint lint-fix \
        format precommit clean run-local

help:
	@echo "GEKTOR APEX — local development commands"
	@echo ""
	@echo "  make install        Install runtime + dev dependencies via pip"
	@echo "  make test           Run full pytest suite"
	@echo "  make test-radar     Run only the regression suite (fast)"
	@echo "  make test-vpin      Run VPIN invariant tests"
	@echo "  make test-pipeline  Run pipeline integration tests"
	@echo "  make lint           ruff check on the radar contour"
	@echo "  make lint-fix       ruff check --fix on the radar contour"
	@echo "  make precommit      Run all pre-commit hooks against staged files"
	@echo "  make run-local      Run the radar in local advisory mode"
	@echo "  make clean          Remove __pycache__ and pytest caches"

install:
	python -m pip install --upgrade pip
	pip install \
		"numpy==1.26.2" \
		"SQLAlchemy==2.0.23" \
		"pydantic==2.5.2" \
		"pydantic-settings==2.1.0" \
		"python-dotenv>=1.0.0" \
		"orjson>=3.9.10" \
		"loguru==0.7.2" \
		"msgspec==0.18.5" \
		"aiohttp[speedups]==3.9.1" \
		"pyyaml==6.0.1" \
		"redis==5.0.1" \
		"aiohttp-socks==0.8.4" \
		"asyncpg==0.29.0" \
		"aiosqlite>=0.19" \
		"hypothesis>=6.100" \
		"pytest==7.4.3" \
		"pytest-asyncio==0.23.2" \
		"freezegun==1.2.2" \
		"ruff==0.1.7" \
		"pre-commit>=3.5"

test:
	python -m pytest tests/ -q --tb=short

test-radar:
	python -m pytest tests/regression/ -q --tb=short

test-vpin:
	python -m pytest tests/regression/test_vpin_invariants.py tests/regression/test_vpin_properties.py -v

test-pipeline:
	python -m pytest tests/regression/test_radar_pipeline.py tests/regression/test_ingestor_integration.py -v

lint:
	ruff check \
		src/domain/vpin_engine.py \
		src/domain/conflation.py \
		src/application/radar_pipeline.py \
		src/application/watchdog.py \
		src/application/outbox_alert_sink.py \
		src/application/outbox_relay.py \
		src/application/formatters.py \
		tests/regression/

lint-fix:
	ruff check --fix \
		src/domain/vpin_engine.py \
		src/domain/conflation.py \
		src/application/radar_pipeline.py \
		src/application/watchdog.py \
		src/application/outbox_alert_sink.py \
		src/application/outbox_relay.py \
		src/application/formatters.py \
		tests/regression/

precommit:
	pre-commit run --all-files

run-local:
	python main.py

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} \;
	find . -type d -name .pytest_cache -prune -exec rm -rf {} \;
	find . -type d -name .hypothesis -prune -exec rm -rf {} \;
	find . -type d -name .ruff_cache -prune -exec rm -rf {} \;
