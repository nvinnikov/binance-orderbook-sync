# Codebase Structure

**Analysis Date:** 2026-04-09

## Directory Layout

```
binance-orderbook-sync/
├── main.py                          # Entire application — models, sync logic, entry point
├── requirements.txt                 # Python dependencies
├── README.md                        # Usage documentation (Russian)
├── CLAUDE.md                        # Project-level Claude instructions
└── .github/
    └── workflows/
        └── run-orderbook.yml        # GitHub Actions workflow (blocked by geo-restriction)
```

## Module Layout

The project is a **single-file application**. All code lives in `main.py`. There are no packages, sub-modules, or generated files.

`main.py` is organized into four clearly commented sections:

| Section | Lines | Content |
|---|---|---|
| `#Env` | 11–15 | Module-level constants and URL construction |
| `#Models` | 18–34 | Pydantic model definitions |
| `# REST Snapshot` | 37–80 | `get_snapshot()` function |
| `# Helpers` | 83–211 | Helper functions (book init, buffer management, update application, display) |
| `# Run` | 213–344 | `handle_websocket()` orchestrator and `__main__` entry point |

## Key Functions and Responsibilities

**`get_snapshot(url, symbol, size) -> SnapshotResponse | None`** (line 40)
- Makes a `GET` request to the Binance Futures REST depth endpoint
- Clamps `size` to the API-valid range `[5, 1000]`
- Returns a validated `SnapshotResponse` or `None` on any error
- Handles: HTTP non-200, `Timeout`, `SSLError`, `ConnectionError`, generic `RequestException`, `ValidationError`, unexpected exceptions

**`init_order_book(snapshot) -> Dict[str, Dict[float, float]]`** (line 85)
- Converts a `SnapshotResponse` into the mutable order book dict
- Parses string prices/quantities to `float`
- Returns `{"bids": {float: float, ...}, "asks": {float: float, ...}}`

**`extract_depth_payload(msg) -> Dict[str, Any]`** (line 92)
- Unwraps the Binance combined-stream envelope (`msg["data"]`) if present
- Falls back to returning `msg` unchanged for single-stream format

**`print_order_book(order_book, top_n) -> None`** (line 106)
- Debug/display only — not in the hot path
- Sorts bids descending and asks ascending for readability
- Prints top N levels with best bid, best ask, and spread

**`check_buffer_with_snapshot(buffer, snapshot_last_id) -> List[DepthUpdateData]`** (line 137)
- Alignment step: finds the first buffered event `e` where `e.U <= snapshot_last_id + 1 <= e.u`
- Returns the buffer slice starting at that event
- Raises `RuntimeError` if no qualifying event exists

**`extend_buffer_until_expected(ws, buffer, snapshot_last_id, max_events) -> List[DepthUpdateData]`** (line 154)
- Called when the snapshot's `lastUpdateId` is ahead of all buffered events
- Reads additional WebSocket messages (up to `max_events`) until the buffer contains an event covering `snapshot_last_id + 1`
- Returns the extended buffer in place
- Raises `RuntimeError` if the safety cap is reached

**`apply_depth_update(order_book, event, lastUpdateId, require_pu) -> int`** (line 183)
- Core mutation function — applies one `DepthUpdateData` to the order book
- Skips stale events (`event.u <= lastUpdateId`)
- When `require_pu=True`, raises `RuntimeError` on `event.pu != lastUpdateId`
- Deletes price levels where `qty == 0.0`; otherwise upserts
- Returns the new `lastUpdateId` (`event.u`)

**`handle_websocket(ws_url, snapshot_url, symbol, snapshot_limit, prebuffer_count, print_every_sec, top_n)`** (line 213)
- Top-level orchestrator — runs the full sync lifecycle: pre-buffer → snapshot → alignment → buffer apply → live loop
- Holds all mutable state as local variables
- Returns (without raising) on any unrecoverable error; prints a message indicating restart is needed
- Called directly from `__main__`

## Entry Point and Configuration

**Entry point:** `main.py` line 335 — standard `if __name__ == "__main__":` block.

**Configuration resolution (priority: highest → lowest):**

1. **CLI argument** — `sys.argv[1]` if provided: `python main.py ethusdt`
2. **Environment variable** — `SYMBOL` env var: `SYMBOL=ethusdt python main.py`
3. **Default value** — `"btcusdt"`

The resolution logic is implied by the README but the actual CLI override mechanism is handled at module level before `handle_websocket()` is called. The module-level constants are:

```python
SYMBOL = os.getenv("SYMBOL", "btcusdt")          # main.py line 13
REST_SNAPSHOT_URL = "https://fapi.binance.com/fapi/v1/depth"   # line 14
WS_URL = f"wss://fstream.binance.com/stream?streams={SYMBOL}@depth"  # line 15
```

**Hardcoded tuning parameters** (set at the `handle_websocket()` call site, line 336):

| Parameter | Default | Meaning |
|---|---|---|
| `snapshot_limit` | `1000` | Depth levels fetched in REST snapshot (max allowed by Binance Futures) |
| `prebuffer_count` | `50` | Minimum WS events to collect before fetching snapshot |
| `print_every_sec` | `1.0` | Console refresh interval in seconds |
| `top_n` | `10` | Price levels displayed per side |

These parameters are not exposed via CLI or env vars; they must be changed directly in `main.py`.

## Naming Conventions

**Files:**
- Single lowercase file: `main.py`

**Functions:**
- `snake_case` throughout

**Variables:**
- `snake_case` for local variables and parameters
- Module-level constants in `UPPER_SNAKE_CASE` (`SYMBOL`, `REST_SNAPSHOT_URL`, `WS_URL`)
- Pydantic model fields mirror Binance API field names verbatim (`lastUpdateId`, `U`, `u`, `pu`, `b`, `a`, `e`, `E`, `s`)

**Types:**
- `PascalCase` for Pydantic model classes (`SnapshotResponse`, `DepthUpdateData`)
- Standard library type hints used throughout (`Dict`, `List`, `Any` from `typing`)

## Where to Add New Code

**New data model (e.g., trade stream):**
- Add a new `BaseModel` subclass in `main.py` under the `#Models` section

**New helper / utility function:**
- Add under the `# Helpers` section in `main.py`

**New sync phase or alternative loop:**
- Extend or fork `handle_websocket()` in the `# Run` section

**If the codebase grows beyond a single file**, natural module boundaries are:
- `models.py` — Pydantic models
- `snapshot.py` — REST snapshot logic
- `orderbook.py` — order book data structure and `apply_depth_update()`
- `sync.py` — `handle_websocket()` orchestrator

## Special Files

**`.github/workflows/run-orderbook.yml`:**
- GitHub Actions workflow
- Non-functional due to Binance geo-restriction (HTTP 451 from GitHub-hosted runners)
- Committed but not usable without a self-hosted runner in a permitted region

---

*Structure analysis: 2026-04-09*
