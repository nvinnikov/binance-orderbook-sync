"""
Plots order book timeseries from the SQLite DB written by analytics/timeseries.py.

Usage:
    python plot.py                      # reads orderbook.db
    python plot.py --db path/to/db
    python plot.py --last 300           # last 300 rows only
"""
import argparse
import sqlite3
from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def load_data(db_path: str, last_n: int | None = None) -> dict:
    conn = sqlite3.connect(db_path)
    if last_n:
        query = (
            "SELECT timestamp, best_bid, best_ask, spread, mid_price, obi, "
            "total_bid_depth_top10, total_ask_depth_top10 "
            "FROM orderbook_timeseries ORDER BY timestamp DESC LIMIT ?"
        )
        rows = list(reversed(conn.execute(query, (last_n,)).fetchall()))
    else:
        query = (
            "SELECT timestamp, best_bid, best_ask, spread, mid_price, obi, "
            "total_bid_depth_top10, total_ask_depth_top10 "
            "FROM orderbook_timeseries ORDER BY timestamp"
        )
        rows = conn.execute(query).fetchall()
    conn.close()

    keys = [
        "timestamp", "best_bid", "best_ask", "spread", "mid_price",
        "obi", "total_bid_depth_top10", "total_ask_depth_top10",
    ]
    data: dict = {k: [] for k in keys}
    for row in rows:
        for k, v in zip(keys, row):
            data[k].append(v)

    data["dt"] = [datetime.fromtimestamp(ts) for ts in data["timestamp"]]
    return data


def plot(db_path: str, last_n: int | None = None) -> None:
    data = load_data(db_path, last_n=last_n)

    if not data["dt"]:
        print(f"No data found in {db_path}.")
        return

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
    fig.suptitle(f"Order Book Analytics — {db_path}  ({len(data['dt'])} rows)", fontsize=13)

    # --- Chart 1: Mid price + spread ---
    ax1 = axes[0]
    ax1.plot(data["dt"], data["mid_price"], label="Mid Price", color="steelblue", linewidth=1)
    ax1.set_ylabel("Price (USDT)")
    ax1.legend(loc="upper left", fontsize=8)

    ax1r = ax1.twinx()
    ax1r.plot(data["dt"], data["spread"], color="orange", linewidth=1, label="Spread")
    spread_vals = [v for v in data["spread"] if v is not None]
    if spread_vals:
        margin = max(max(spread_vals) * 0.5, 0.01)
        ax1r.set_ylim(min(spread_vals) - margin, max(spread_vals) + margin)
    ax1r.set_ylabel("Spread")
    ax1r.legend(loc="upper right", fontsize=8)

    # --- Chart 2: OBI ---
    ax2 = axes[1]
    ax2.plot(data["dt"], data["obi"], label="OBI", color="purple", linewidth=1)
    ax2.axhline(0.7, color="red", linestyle="--", linewidth=0.8, alpha=0.6, label="Alert +0.7")
    ax2.axhline(-0.7, color="blue", linestyle="--", linewidth=0.8, alpha=0.6, label="Alert -0.7")
    ax2.axhline(0.0, color="gray", linestyle=":", linewidth=0.6, alpha=0.4)
    ax2.set_ylim(-1.1, 1.1)
    ax2.set_ylabel("OBI")
    ax2.legend(loc="upper left", fontsize=8)

    # --- Chart 3: Bid / Ask depth top 10 ---
    ax3 = axes[2]
    ax3.fill_between(
        data["dt"], data["total_bid_depth_top10"],
        alpha=0.4, color="green", label="Bid Depth Top10",
    )
    ax3.fill_between(
        data["dt"], data["total_ask_depth_top10"],
        alpha=0.4, color="red", label="Ask Depth Top10",
    )
    ax3.set_ylabel("Depth (qty)")
    ax3.legend(loc="upper left", fontsize=8)

    fmt = mdates.DateFormatter("%H:%M:%S")
    for ax in axes:
        ax.xaxis.set_major_formatter(fmt)
        ax.grid(True, alpha=0.2)

    plt.xlabel("Time")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Plot order book timeseries from SQLite")
    parser.add_argument("--db", default="orderbook.db", help="SQLite DB path (default: orderbook.db)")
    parser.add_argument("--last", type=int, default=None, help="Show only last N rows")
    args = parser.parse_args()
    plot(args.db, last_n=args.last)
