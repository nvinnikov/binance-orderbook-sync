# Architecture

**Analysis Date:** 2026-04-09

## Pattern Overview

**Overall:** Single-process sequential sync algorithm following the Binance Futures diff depth stream specification.

**Key Characteristics:**
- WebSocket-first: stream is opened before any REST call is made, ensuring no gap between buffered and live events
- Pre-buffering: a fixed number of WebSocket events are accumulated before the REST snapshot is fetched, guaranteeing the buffer overlaps the snapshot
- Strict sequential consistency enforced via the `pu` field after the first applied event
- No threading or async I/O — `websockets.sync.client` is used for a synchronous blocking loop
- On any continuity violation the function returns (caller must restart the entire process)

## Sync Algorithm — Phase by Phase

**Phase 1 — Pre-buffer:**
- Open WebSocket connection
- Receive and parse events until `buffer` contains at least `prebuffer_count` (default 50) `DepthUpdateData` items
- Non-`depthUpdate` messages are silently skipped
- After this phase the buffer holds a contiguous window of update IDs that predates the forthcoming snapshot

**Phase 2 — REST Snapshot:**
- Call `get_snapshot()` against `https://fapi.binance.com/fapi/v1/depth`
- Receive `SnapshotResponse` with `lastUpdateId`, `bids`, `asks`
- If the snapshot's `lastUpdateId` is ahead of the entire buffer (`buffer[-1].u <= snapshot_last_id`), call `extend_buffer_until_expected()` to keep consuming the live WebSocket until an event whose range covers `snapshot_last_id + 1` is received (safety cap: 20 000 extra events)
- If no matching event is found within the cap, the function returns

**Phase 3 — Alignment:**
- Call `check_buffer_with_snapshot(buffer, snapshot_last_id)` to find the first buffered event `e` where `e.U <= snapshot_last_id + 1 <= e.u` and discard all earlier events
- If no such event exists, `RuntimeError` is raised and the function returns

**Phase 4 — Buffer Apply:**
- Initialize the local order book from snapshot data via `init_order_book()`
- Set `lastUpdateId = snapshot.lastUpdateId`
- Iterate over the trimmed buffer:
  - Skip events where `event.u <= lastUpdateId` (already covered by snapshot)
  - For the **first** applied event: verify `event.U <= lastUpdateId + 1 <= event.u`; apply with `require_pu=False` (Binance does not guarantee `pu` on the alignment event)
  - For all **subsequent** events: apply with `require_pu=True`, which enforces `event.pu == lastUpdateId`
- Any violation raises `RuntimeError` and the function returns

**Phase 5 — Live Loop:**
- Enters an infinite `while True` loop
- Every received `depthUpdate` is parsed and applied via `apply_depth_update()` with `require_pu=True`
- A continuity break causes `RuntimeError`, which exits the loop and returns from `handle_websocket()`
- `print_order_book()` fires at most once per `print_every_sec` seconds (throttled via `time.time()`)

## Data Flow

**Inbound WebSocket message path:**

1. `ws.recv()` returns raw JSON string
2. `json.loads()` deserializes to `dict`
3. `extract_depth_payload(msg)` extracts `msg["data"]` when the multiplexed stream wrapper is present, otherwise returns `msg` unchanged
4. `payload.get("e") != "depthUpdate"` guard skips non-book messages
5. `DepthUpdateData(**payload)` validates via Pydantic
6. `apply_depth_update()` mutates the order book dict in place and returns the new `lastUpdateId`

**State held in `handle_websocket()` local scope:**

| Variable | Type | Purpose |
|---|---|---|
| `buffer` | `List[DepthUpdateData]` | Pre-snapshot event queue |
| `order_book` | `Dict[str, Dict[float, float]]` | Live book: `{"bids": {price: qty}, "asks": {price: qty}}` |
| `lastUpdateId` | `int` | Last applied `u` value; the continuity anchor |
| `last_print_ts` | `float` | Unix timestamp of last console print |

## Order Book Data Structure

The local order book is a plain Python `dict` with two sub-dicts:

```python
{
    "bids": {float(price): float(qty), ...},
    "asks": {float(price): float(qty), ...},
}
```

- Keys are `float` (converted from Binance's string prices)
- Values are `float` quantities
- A qty of `0.0` in an update means remove the price level: `order_book["bids"].pop(price, None)`
- The dict is **unsorted** by design — the comment in `print_order_book()` explicitly notes that production use would replace this with a sorted structure (tree/heap)

## Pydantic Models

**`SnapshotResponse`** (`main.py` lines 20-23):
- Validates the REST snapshot response
- Fields: `lastUpdateId: int`, `bids: List[List[str]]`, `asks: List[List[str]]`
- `bids`/`asks` are lists of `[price_string, qty_string]` pairs — strings are kept as-is; conversion to `float` happens in `init_order_book()`

**`DepthUpdateData`** (`main.py` lines 26-34):
- Validates each WebSocket depth event
- Key fields:
  - `U: int` — first update ID in this event
  - `u: int` — final update ID in this event
  - `pu: int` — final update ID of the **previous** event (Binance Futures specific; absent on Binance Spot)
  - `b: List[List[str]]` — bid updates as `[price_str, qty_str]` pairs
  - `a: List[List[str]]` — ask updates as `[price_str, qty_str]` pairs
- `ValidationError` from Pydantic causes the offending event to be skipped (logged, not fatal) during buffering; in the live loop a validation error is also non-fatal (`continue`)

## `pu` Continuity Enforcement

After the alignment event is applied, every subsequent event must satisfy:

```
event.pu == lastUpdateId
```

This is enforced inside `apply_depth_update()` when `require_pu=True`:

```python
if require_pu and event.pu != lastUpdateId:
    raise RuntimeError(f"Missed events: event.pu={event.pu} != lastUpdateId={lastUpdateId}")
```

The first aligned event is always applied with `require_pu=False` because Binance's own documentation states that `pu` of the alignment event equals the `lastUpdateId` of the snapshot, not the previous stream event — it is the first event Binance guarantees will overlap the snapshot, not a strict chain continuation.

After the first application, `require_pu=True` is used for all remaining buffer events and throughout the entire live loop. Any gap causes immediate termination.

## Error Handling / Restart Strategy

There is no automatic retry or reconnect logic inside `handle_websocket()`. Failure modes and their response:

| Failure | Behaviour |
|---|---|
| Snapshot HTTP non-200 | Log and return `None` from `get_snapshot()`; `handle_websocket()` returns |
| Snapshot timeout / SSL / connection error | Log and return `None`; `handle_websocket()` returns |
| Snapshot `ValidationError` | Log and return `None`; `handle_websocket()` returns |
| Buffer can't reach snapshot `lastUpdateId` within 20 000 events | Log and return from `handle_websocket()` |
| No buffer event covers `snapshot_last_id + 1` | `RuntimeError` caught; log and return |
| First buffer event doesn't cover expected ID | `RuntimeError` caught; log and return |
| `pu` mismatch in buffer or live loop | `RuntimeError` from `apply_depth_update()`; caught; log and return |
| `ValidationError` on live event | Logged; `continue` (non-fatal, event skipped) |

The `__main__` block calls `handle_websocket()` once and exits when it returns. There is no outer retry loop — restarts must be handled externally (e.g., process supervisor, shell loop, GitHub Actions retry).

---

*Architecture analysis: 2026-04-09*
