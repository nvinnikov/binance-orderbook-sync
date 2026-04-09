# Binance Futures Order Book Analyzer

Real-time order book reconstruction and microstructure analytics for Binance Futures — built to surface liquidity signals before price moves, not just stream raw data.

## What it does

The core engine maintains a locally-consistent order book by following the official [Binance Futures diff depth stream specification](https://binance-docs.github.io/apidocs/futures/en/#diff-book-depth-streams): pre-buffering WebSocket events before the REST snapshot, aligning on the correct sequence anchor (`U ≤ lastUpdateId+1 ≤ u`), validating `pu` continuity on every subsequent event, and triggering a clean restart on any detected gap. The result is a book you can trust.

On top of that runs an analytics layer, fully isolated from the sync internals:

- **OBI (Order Book Imbalance)** — `(bid_vol − ask_vol) / (bid_vol + ask_vol)` across the top N levels, updated on every tick. Values approaching ±1 signal aggressive one-sided pressure.
- **Whale wall detection** — flags new large resting orders where `qty ≥ mean_qty × threshold`. Tracks state across ticks so repeated alerts aren't generated for the same wall.
- **Depth timeseries** — writes best bid/ask, spread, mid price, OBI, and top-10 bid/ask depth to SQLite at a configurable interval. Queryable and plottable offline.

## Why it matters

Tape readers and quant analysts care about order book dynamics, not just price. OBI at extreme values often precedes short-term directional moves. Large passive walls telegraph where market makers and large participants are defending levels. Depth imbalance across time tells you whether the move has backing or is a liquidity vacuum. This project treats the book as a signal source, not just a display.

## Example output

```
========================================================================
ORDER BOOK (top 10) | best_bid=77899.0 best_ask=77899.1 spread=0.1
------------------------------------------------------------------------
BIDS (price, qty)                  | ASKS (price, qty)
------------------------------------------------------------------------
77899.00  3.011000                 | 77899.10  0.175000
77898.90  0.021000                 | 77899.20  0.008000
77898.80  0.003000                 | 77899.40  0.003000
77898.70  0.002000                 | 77899.80  0.003000
77898.60  0.006000                 | 77899.90  0.004000
77898.50  0.003000                 | 77900.00  0.002000
77898.40  0.002000                 | 77900.10  0.002000
77898.10  0.006000                 | 77900.20  0.005000
77898.00  0.002000                 | 77900.60  0.005000
77897.90  0.002000                 | 77900.70  0.004000
========================================================================
```

![Timeseries visualization: mid price, OBI, bid/ask depth](img_1.png)

## Stack

Python 3.11 · `websockets` · `pydantic` · `requests` · `sqlite3` · `matplotlib`

## Usage

```bash
pip install -r requirements.txt

# Run with default symbol (btcusdt)
python main.py

# Override symbol via env or CLI arg (CLI takes priority)
SYMBOL=ethusdt python main.py
python main.py ethusdt
```

Plot the recorded timeseries:

```bash
python plot.py                  # full dataset from orderbook.db
python plot.py --last 300       # last 300 rows
python plot.py --db path/to/db
```

Run tests:

```bash
pytest tests/
```

## Implementation notes

The sync algorithm follows the Binance Futures diff depth stream specification precisely: WebSocket connection is opened first and events are buffered before the REST snapshot is requested. On snapshot receipt, the buffer is scanned for the first event where `U ≤ snapshot_lastUpdateId+1 ≤ u`. If the snapshot has advanced past the buffer, additional events are consumed until coverage is found. The first overlapping event is applied without `pu` validation; all subsequent events enforce `event.pu == lastUpdateId`. Any continuity break raises an error and signals a full restart — no silent state corruption.

The analytics layer (`analytics/`) receives the reconstructed book as a plain dict and has no access to sync state. `obi.py`, `whale.py`, and `timeseries.py` are independently testable.

## CI

A GitHub Actions workflow (`.github/workflows/run-orderbook.yml`) runs the script for 5 minutes and uploads the log as an artifact. Note: Binance API is geo-restricted in GitHub-hosted runners and returns HTTP 451. The workflow is useful for log inspection in permitted regions but not for functional CI.
