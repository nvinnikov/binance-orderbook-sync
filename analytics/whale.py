# analytics/whale.py
import logging
from typing import Dict, List, Set

logger = logging.getLogger(__name__)


def _find_whale_walls(
    order_book: Dict[str, Dict[float, float]],
    threshold: float = 10.0,
) -> List[Dict]:
    """
    Returns list of {"side": "BID"|"ASK", "price": float, "qty": float}
    for all levels where qty >= mean_qty * threshold.
    """
    all_qtys = list(order_book["bids"].values()) + list(order_book["asks"].values())
    if not all_qtys:
        return []

    mean_qty = sum(all_qtys) / len(all_qtys)
    cutoff = mean_qty * threshold

    walls = []
    for price, qty in order_book["bids"].items():
        if qty >= cutoff:
            walls.append({"side": "BID", "price": price, "qty": qty})
    for price, qty in order_book["asks"].items():
        if qty >= cutoff:
            walls.append({"side": "ASK", "price": price, "qty": qty})

    return walls


class WhaleDetector:
    """Tracks previously seen whale walls and logs only newly appeared ones."""

    def __init__(self, threshold: float = 10.0):
        self.threshold = threshold
        self._prev_keys: Set[tuple] = set()

    def scan(self, order_book: Dict[str, Dict[float, float]], mid_price: float) -> None:
        walls = _find_whale_walls(order_book, self.threshold)
        current_keys = {(w["side"], w["price"]) for w in walls}

        new_walls = [w for w in walls if (w["side"], w["price"]) not in self._prev_keys]

        for wall in new_walls:
            dist_pct = abs(wall["price"] - mid_price) / mid_price * 100 if mid_price else 0.0
            logger.warning(
                "WHALE WALL | %s  price=%.2f  qty=%.4f  dist_from_mid=%.2f%%",
                wall["side"],
                wall["price"],
                wall["qty"],
                dist_pct,
            )

        self._prev_keys = current_keys
