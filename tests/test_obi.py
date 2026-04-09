from analytics.obi import compute_obi


def test_obi_balanced():
    order_book = {
        "bids": {100.0: 5.0, 99.0: 3.0},
        "asks": {101.0: 5.0, 102.0: 3.0},
    }
    assert compute_obi(order_book, n=10) == 0.0


def test_obi_all_bids():
    order_book = {"bids": {100.0: 10.0}, "asks": {}}
    assert compute_obi(order_book, n=10) == 1.0


def test_obi_all_asks():
    order_book = {"bids": {}, "asks": {101.0: 10.0}}
    assert compute_obi(order_book, n=10) == -1.0


def test_obi_top_n_respected():
    # top 1 bid = 10, top 1 ask = 10 → balanced
    order_book = {
        "bids": {100.0: 10.0, 99.0: 999.0},
        "asks": {101.0: 10.0, 102.0: 999.0},
    }
    assert compute_obi(order_book, n=1) == 0.0


def test_obi_empty_returns_zero():
    assert compute_obi({"bids": {}, "asks": {}}, n=10) == 0.0


def test_obi_range():
    order_book = {
        "bids": {100.0: 7.0},
        "asks": {101.0: 3.0},
    }
    result = compute_obi(order_book, n=10)
    assert -1.0 <= result <= 1.0
    assert abs(result - 0.4) < 1e-9  # (7-3)/(7+3) = 0.4
