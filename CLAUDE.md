# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run with default symbol (btcusdt)
python main.py

# Run with a specific symbol (CLI arg takes priority over env var)
python main.py ethusdt
SYMBOL=ethusdt python main.py
```

> **Note**: GitHub Actions cannot reach Binance API due to geo-restrictions (HTTP 451). Run locally or on a server in an allowed region.

## Architecture

Single-file application (`main.py`) implementing the [Binance Futures order book sync algorithm](https://binance-docs.github.io/apidocs/futures/en/#diff-book-depth-streams):

1. **Pre-buffer phase**: Connect to WebSocket (`wss://fstream.binance.com`) and collect `prebuffer_count` (default 50) depth events before fetching the snapshot, ensuring no gap between snapshot and live stream.

2. **Snapshot phase**: Fetch REST snapshot from `https://fapi.binance.com/fapi/v1/depth` with `lastUpdateId`.

3. **Buffer alignment**: Find the first buffered event where `event.U <= snapshot_last_id + 1 <= event.u`. If snapshot is ahead of buffer, extend buffer from WS until coverage is found (`extend_buffer_until_expected`).

4. **Apply buffer**: First buffered event is applied without `pu` check; subsequent events enforce `event.pu == lastUpdateId` to detect missed events.

5. **Live streaming loop**: Apply incoming WS events with strict `pu` continuity check. If continuity breaks, the whole process must restart.

**Order book storage**: Plain Python dicts `{"bids": {price: qty}, "asks": {price: qty}}` keyed by `float`. The `print_order_book` function sorts for display only — intentionally not part of the hot path. A production system would use a sorted structure (tree/heap).

**Pydantic models**: `SnapshotResponse` validates REST response; `DepthUpdateData` validates each WS depth event. Validation errors are caught and logged without crashing.

**Symbol configuration**: `SYMBOL` env var → default `btcusdt`. CLI arg (`sys.argv[1]`) overrides env var.
