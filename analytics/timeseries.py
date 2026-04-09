# analytics/timeseries.py
import sqlite3
import time
from typing import Dict

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS orderbook_timeseries (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp             REAL    NOT NULL,
    best_bid              REAL,
    best_ask              REAL,
    spread                REAL,
    mid_price             REAL,
    obi                   REAL,
    total_bid_depth_top10 REAL,
    total_ask_depth_top10 REAL
)
"""

_INSERT_SQL = """
INSERT INTO orderbook_timeseries
    (timestamp, best_bid, best_ask, spread, mid_price, obi,
     total_bid_depth_top10, total_ask_depth_top10)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


class TimeseriesRecorder:
    """Writes order book snapshots to SQLite at a configurable interval."""

    def __init__(self, db_path: str = "orderbook.db", interval_sec: float = 1.0):
        self.interval_sec = interval_sec
        self._conn = sqlite3.connect(db_path)
        self._conn.execute(_CREATE_TABLE_SQL)
        self._conn.commit()
        self._last_record_ts: float = 0.0

    def record(self, order_book: Dict[str, Dict[float, float]], obi: float) -> None:
        """Write one row if enough time has elapsed since last write."""
        now = time.time()
        if now - self._last_record_ts < self.interval_sec:
            return

        bids = sorted(order_book["bids"].items(), key=lambda x: x[0], reverse=True)[:10]
        asks = sorted(order_book["asks"].items(), key=lambda x: x[0])[:10]

        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None
        mid_price = ((best_bid + best_ask) / 2) if (best_bid is not None and best_ask is not None) else None
        total_bid = sum(q for _, q in bids)
        total_ask = sum(q for _, q in asks)

        self._conn.execute(_INSERT_SQL, (now, best_bid, best_ask, spread, mid_price, obi, total_bid, total_ask))
        self._conn.commit()
        self._last_record_ts = now

    def close(self) -> None:
        self._conn.close()
