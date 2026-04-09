# External Integrations

**Analysis Date:** 2026-04-09

## APIs & External Services

**Binance Futures REST API:**
- Service: Binance Futures (`fapi.binance.com`)
- Purpose: Fetch a point-in-time order book snapshot to seed the local book
- Endpoint: `GET https://fapi.binance.com/fapi/v1/depth`
- Parameters: `symbol` (uppercased), `limit` (5–1000, default 1000)
- Auth: None — public endpoint
- Client: `requests>=2.31`
- Implementation: `get_snapshot()` in `main.py:40-80`
- Response model: `SnapshotResponse` (pydantic) — fields `lastUpdateId`, `bids`, `asks`
- Error handling: Timeout (10s), SSLError, ConnectionError, generic RequestException, and ValidationError are all caught and logged; returns `None` on failure

**Binance Futures WebSocket Stream:**
- Service: Binance Futures stream (`fstream.binance.com`)
- Purpose: Receive real-time incremental depth (order book delta) events
- URL: `wss://fstream.binance.com/stream?streams={SYMBOL}@depth`
- Auth: None — public endpoint
- Client: `websockets>=11.0` using synchronous API (`websockets.sync.client.connect`)
- Implementation: `handle_websocket()` in `main.py:213-330`
- Message model: `DepthUpdateData` (pydantic) — fields `e`, `E`, `s`, `U` (firstUpdateId), `u` (finalUpdateId), `pu` (prev finalUpdateId), `b` (bid deltas), `a` (ask deltas)
- Messages arrive wrapped in `{"data": {...}}` envelope; `extract_depth_payload()` unwraps them (`main.py:92-93`)

## Data Flow

**Sync Algorithm (Binance Futures diff-depth protocol):**

1. **Pre-buffer phase** (`main.py:226-239`): Connect to WebSocket first. Collect `prebuffer_count` (default 50) `depthUpdate` events into a buffer before fetching the REST snapshot. This ensures no gap exists between snapshot state and live stream.

2. **Snapshot fetch** (`main.py:242-249`): Call `GET fapi.binance.com/fapi/v1/depth` to get `lastUpdateId` and initial bids/asks.

3. **Buffer alignment** (`main.py:252-267`): If the snapshot `lastUpdateId` is ahead of all buffered events, extend the buffer by reading more WebSocket messages (`extend_buffer_until_expected`, `main.py:154-180`). Then find the first buffered event satisfying `event.U <= snapshot_last_id + 1 <= event.u` (`check_buffer_with_snapshot`, `main.py:137-151`).

4. **Apply buffer** (`main.py:276-299`): Apply matching buffered events to the local order book. The first event is applied without `pu` continuity check; all subsequent events enforce `event.pu == lastUpdateId` to detect any gaps.

5. **Live streaming loop** (`main.py:307-330`): Enter perpetual loop receiving WebSocket messages. Each `depthUpdate` event is validated and applied with strict `pu` continuity (`apply_depth_update`, `main.py:183-211`). A continuity break causes the function to return (caller must restart the full process). Order book is printed to stdout at most once per second (`print_every_sec=1.0`).

**Order book state:**
- Stored in plain Python dicts: `{"bids": {float_price: float_qty}, "asks": {float_price: float_qty}}`
- Update rule: `qty == 0.0` removes the price level; any non-zero qty upserts it

## Data Storage

**Databases:** None

**File Storage:**
- No persistent storage — order book lives in-process memory only
- CI run uploads `orderbook.log` (stdout capture) as a GitHub Actions artifact

**Caching:** None

## Authentication & Identity

**Auth Provider:** None — all Binance endpoints used are public and require no API key or signature

## Monitoring & Observability

**Error Tracking:** None — errors are printed to stdout via `print()` only

**Logs:**
- stdout only; GitHub Actions CI captures output to `orderbook.log` via `tee`
- No structured logging library in use

## CI/CD & Deployment

**Hosting:**
- No production hosting detected — designed to run locally or on a user-managed server in a Binance-accessible region

**CI Pipeline:**
- GitHub Actions workflow: `.github/workflows/run-orderbook.yml`
- Trigger: `workflow_dispatch` (manual), with `symbol` input choice (`btcusdt` / `ethusdt`)
- Runner: `ubuntu-latest`, timeout 10 minutes
- Process is run for exactly 300 seconds (`timeout 300s python main.py`)
- Log artifact uploaded as `orderbook-log` after every run

**Note:** GitHub Actions runners cannot reach Binance API endpoints due to geo-restrictions (HTTP 451). The workflow is non-functional from default GitHub-hosted runners and is intended for documentation/demonstration purposes or self-hosted runners in allowed regions.

## Environment Configuration

**Required env vars:**
- `SYMBOL` — trading pair (e.g., `btcusdt`, `ethusdt`); defaults to `btcusdt` if unset

**No secrets required** — no API keys, tokens, or credentials of any kind

## Webhooks & Callbacks

**Incoming:** None

**Outgoing:** None

---

*Integration audit: 2026-04-09*
