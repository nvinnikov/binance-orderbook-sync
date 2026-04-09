import logging
from analytics.whale import WhaleDetector, _find_whale_walls


def _make_book(bid_items, ask_items):
    return {
        "bids": dict(bid_items),
        "asks": dict(ask_items),
    }


def test_no_whales_when_below_threshold():
    book = _make_book([(100.0, 1.0), (99.0, 1.0)], [(101.0, 1.0)])
    walls = _find_whale_walls(book, threshold=10.0)
    assert walls == []


def test_detects_bid_whale():
    # mean = (100+1+1)/3 ≈ 34, threshold=2 → cutoff ≈ 68; 100 > 68
    book = _make_book([(100.0, 100.0), (99.0, 1.0)], [(101.0, 1.0)])
    walls = _find_whale_walls(book, threshold=2.0)
    assert len(walls) == 1
    assert walls[0]["side"] == "BID"
    assert walls[0]["price"] == 100.0
    assert walls[0]["qty"] == 100.0


def test_detects_ask_whale():
    book = _make_book([(100.0, 1.0)], [(101.0, 100.0), (102.0, 1.0)])
    walls = _find_whale_walls(book, threshold=2.0)
    assert any(w["side"] == "ASK" and w["price"] == 101.0 for w in walls)


def test_empty_book_returns_no_whales():
    walls = _find_whale_walls({"bids": {}, "asks": {}}, threshold=10.0)
    assert walls == []


def test_new_whale_is_logged(caplog):
    # mean=(100+1)/2=50.5, threshold=1.5 → cutoff=75.75; 100 > 75.75 ✓
    book = _make_book([(100.0, 100.0)], [(101.0, 1.0)])
    detector = WhaleDetector(threshold=1.5)
    with caplog.at_level(logging.WARNING, logger="analytics.whale"):
        detector.scan(book, mid_price=100.5)
    assert "WHALE WALL" in caplog.text
    assert "BID" in caplog.text


def test_existing_whale_not_relogged(caplog):
    book = _make_book([(100.0, 100.0)], [(101.0, 1.0)])
    detector = WhaleDetector(threshold=1.5)
    detector.scan(book, mid_price=100.5)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="analytics.whale"):
        detector.scan(book, mid_price=100.5)
    assert len(caplog.records) == 0


def test_disappeared_whale_then_reappears_logs_again(caplog):
    book_with = _make_book([(100.0, 100.0)], [(101.0, 1.0)])
    book_without = _make_book([(100.0, 1.0)], [(101.0, 1.0)])
    detector = WhaleDetector(threshold=1.5)
    detector.scan(book_with, mid_price=100.5)
    detector.scan(book_without, mid_price=100.5)
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="analytics.whale"):
        detector.scan(book_with, mid_price=100.5)
    assert "WHALE WALL" in caplog.text
