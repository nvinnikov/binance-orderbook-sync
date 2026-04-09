from typing import Dict


def compute_obi(order_book: Dict[str, Dict[float, float]], n: int = 10) -> float:
    """
    Order Book Imbalance: (bid_sum - ask_sum) / (bid_sum + ask_sum)
    Uses top-N levels. Returns value in [-1.0, 1.0].
    Returns 0.0 if total quantity is zero.
    """
    bids = sorted(order_book["bids"].items(), key=lambda x: x[0], reverse=True)[:n]
    asks = sorted(order_book["asks"].items(), key=lambda x: x[0])[:n]

    bid_sum = sum(q for _, q in bids)
    ask_sum = sum(q for _, q in asks)
    total = bid_sum + ask_sum

    if total == 0.0:
        return 0.0

    return (bid_sum - ask_sum) / total
