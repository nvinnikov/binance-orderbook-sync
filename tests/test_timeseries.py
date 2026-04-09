import sqlite3
from analytics.timeseries import TimeseriesRecorder


def _make_book():
    return {
        "bids": {100.0: 5.0, 99.0: 3.0},
        "asks": {101.0: 4.0, 102.0: 2.0},
    }


def test_record_writes_one_row(tmp_path):
    db_path = str(tmp_path / "test.db")
    recorder = TimeseriesRecorder(db_path=db_path, interval_sec=0.0)
    recorder.record(_make_book(), obi=0.1)
    recorder.close()

    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT * FROM orderbook_timeseries").fetchall()
    conn.close()

    assert len(rows) == 1


def test_record_correct_values(tmp_path):
    db_path = str(tmp_path / "test.db")
    recorder = TimeseriesRecorder(db_path=db_path, interval_sec=0.0)
    recorder.record(_make_book(), obi=0.25)
    recorder.close()

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT best_bid, best_ask, spread, mid_price, obi, "
        "total_bid_depth_top10, total_ask_depth_top10 "
        "FROM orderbook_timeseries"
    ).fetchone()
    conn.close()

    best_bid, best_ask, spread, mid_price, obi, bid_depth, ask_depth = row
    assert best_bid == 100.0
    assert best_ask == 101.0
    assert abs(spread - 1.0) < 1e-9
    assert abs(mid_price - 100.5) < 1e-9
    assert abs(obi - 0.25) < 1e-9
    assert abs(bid_depth - 8.0) < 1e-9   # 5+3
    assert abs(ask_depth - 6.0) < 1e-9   # 4+2


def test_record_respects_interval(tmp_path):
    db_path = str(tmp_path / "test.db")
    recorder = TimeseriesRecorder(db_path=db_path, interval_sec=60.0)
    recorder.record(_make_book(), obi=0.0)
    recorder.record(_make_book(), obi=0.0)  # skipped — too soon
    recorder.close()

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM orderbook_timeseries").fetchone()[0]
    conn.close()

    assert count == 1


def test_multiple_records_accumulate(tmp_path):
    db_path = str(tmp_path / "test.db")
    recorder = TimeseriesRecorder(db_path=db_path, interval_sec=0.0)
    for _ in range(5):
        recorder.record(_make_book(), obi=0.0)
    recorder.close()

    conn = sqlite3.connect(db_path)
    count = conn.execute("SELECT COUNT(*) FROM orderbook_timeseries").fetchone()[0]
    conn.close()

    assert count == 5


def test_table_created_on_init(tmp_path):
    db_path = str(tmp_path / "test.db")
    recorder = TimeseriesRecorder(db_path=db_path, interval_sec=0.0)
    recorder.close()

    conn = sqlite3.connect(db_path)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    conn.close()

    assert ("orderbook_timeseries",) in tables
