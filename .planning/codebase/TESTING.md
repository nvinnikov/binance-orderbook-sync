# Testing Patterns

**Analysis Date:** 2026-04-09

## Test Framework

**Runner:** None — no test framework is installed or configured.

No `pytest`, `unittest`, `nose`, or any other test runner is present. `requirements.txt` lists only three runtime dependencies:
```
requests>=2.31
pydantic>=1.10
websockets>=11.0
```

No `pytest.ini`, `setup.cfg`, `pyproject.toml`, `conftest.py`, or `tox.ini` exist.

**Run Commands:**
```bash
# There are no test commands. The only runnable entry point is:
python main.py              # Run with default symbol (btcusdt)
python main.py ethusdt      # Run with specific symbol
SYMBOL=ethusdt python main.py
```

## Test File Organization

**No test files exist.** A search for any file matching `*test*` or `*spec*` returns zero results. The entire project is a single file:

```
binance-orderbook-sync/
├── main.py                          # All application logic
├── requirements.txt                 # Runtime deps only
├── CLAUDE.md                        # Project guidance
├── README.md
└── .github/workflows/run-orderbook.yml
```

## Current Coverage

**Automated test coverage: 0%.**

No unit tests, integration tests, or end-to-end tests of any kind are present.

## How the Code Is Verified Manually

The only verification mechanism is running `main.py` directly and observing stdout. The `print_order_book` function renders a formatted order book table to terminal every `print_every_sec` seconds (default: 1.0s):

```
========================================================================
ORDER BOOK (top 10) | best_bid=104200.50 best_ask=104200.60 spread=0.1
------------------------------------------------------------------------
BIDS (price, qty)                  | ASKS (price, qty)
------------------------------------------------------------------------
104200.50  1.234567                | 104200.60  0.987654
...
========================================================================
```

A human operator visually confirms the spread is reasonable, prices are updating, and the process does not crash. This is the intended validation approach for the current codebase.

## CI/CD via GitHub Actions

**Workflow file:** `.github/workflows/run-orderbook.yml`

**Trigger:** Manual only (`workflow_dispatch`) — no push or PR triggers.

**What it does:**
1. Checks out the repository
2. Sets up Python 3.11
3. Installs dependencies via `pip install -r requirements.txt`
4. Runs `python main.py` for up to 5 minutes (`timeout 300s`), capturing stdout+stderr to `orderbook.log`
5. Uploads `orderbook.log` as a build artifact (retained even on failure, via `if: always()`)

**Symbol selection:** A dropdown input (`choice` type) with options `btcusdt` / `ethusdt`. Defaults to `btcusdt`.

**Key limitation — geo-restriction:** GitHub Actions runners (`ubuntu-latest`) are blocked by Binance with HTTP 451. The workflow will fail at the `get_snapshot` step because Binance Futures API is geo-restricted from GitHub's IP ranges. The CLAUDE.md documents this explicitly:

> GitHub Actions cannot reach Binance API due to geo-restrictions (HTTP 451). Run locally or on a server in an allowed region.

This means the CI workflow functions as a **smoke test for dependency installation only** — it cannot actually validate the order book synchronization logic in the GitHub Actions environment.

## What Could Be Tested (Currently Untested)

The following pure functions in `main.py` have no external dependencies and are directly unit-testable:

**`init_order_book(snapshot)`** — `src: main.py:85`
- Input: a `SnapshotResponse` with string price/qty pairs
- Output: `{"bids": {float: float}, "asks": {float: float}}`
- Testable: yes, no I/O

**`extract_depth_payload(msg)`** — `src: main.py:92`
- Returns `msg.get("data", msg)`
- Testable: trivial

**`check_buffer_with_snapshot(buffer, snapshot_last_id)`** — `src: main.py:137`
- Pure function with list input and slice return or `RuntimeError`
- Testable: yes, with synthetic `DepthUpdateData` instances

**`apply_depth_update(order_book, event, lastUpdateId, require_pu)`** — `src: main.py:183`
- Mutates `order_book` dict in-place, returns new `lastUpdateId`
- Testable: yes, no I/O; covers bid/ask add, bid/ask remove (qty == 0), missed-event detection

**Pydantic models** — `src: main.py:20-34`
- `SnapshotResponse` and `DepthUpdateData` validation can be tested with valid and invalid dicts

**`get_snapshot`** and **`handle_websocket`** require HTTP and WebSocket connections and would need mocking (`unittest.mock`, `responses`, or `pytest-httpx`) to test without network access.

## Recommended Test Setup (If Added)

If tests are introduced, the conventional layout for this project would be:

```
tests/
├── test_order_book.py       # Unit tests for pure functions
├── test_models.py           # Pydantic model validation tests
└── conftest.py              # Shared fixtures
```

Suggested framework: `pytest` with `pytest-cov` for coverage reporting.

```bash
pip install pytest pytest-cov
pytest tests/ -v --cov=main --cov-report=term-missing
```

---

*Testing analysis: 2026-04-09*
