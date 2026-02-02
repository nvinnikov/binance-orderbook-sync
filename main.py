import json
import os
import sys
import time
from typing import Any, Dict, List
import requests
from pydantic import BaseModel, ValidationError
from websockets.sync.client import connect


#Env

SYMBOL = os.getenv("SYMBOL", "btcusdt")
# Поддержка аргумента командной строки (приоритет выше, чем env var)
if len(sys.argv) > 1:
    SYMBOL = sys.argv[1]

REST_SNAPSHOT_URL = "https://fapi.binance.com/fapi/v1/depth"
WS_URL = f"wss://fstream.binance.com/stream?streams={SYMBOL}@depth"


#Models

class SnapshotResponse(BaseModel):
    lastUpdateId: int
    bids: List[List[str]]  # [[price, qty], ...]
    asks: List[List[str]]


class DepthUpdateData(BaseModel):
    e: str  # "depthUpdate"
    E: int
    s: str
    U: int  # firstUpdateId
    u: int  # finalUpdateId
    pu: int
    b: List[List[str]]  # bids updates
    a: List[List[str]]  # asks updates


# REST Snapshot


def get_snapshot(url: str, symbol: str, size: int = 1000) -> SnapshotResponse | None:
    # Binance Futures API limit: 5..1000
    if size > 1000:
        size = 1000
    elif size < 5:
        size = 5

    params = {"symbol": symbol.upper(), "limit": size}

    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code != 200:
            print(f"Snapshot HTTP {resp.status_code}")
            print(f"Body: {resp.text[:300]}")
            return None

        return SnapshotResponse(**resp.json())

    except requests.exceptions.Timeout:
        print("Snapshot error: Timeout")
        return None

    except requests.exceptions.SSLError as e:
        print(f"Snapshot error: SSLError: {e}")
        return None

    except requests.exceptions.ConnectionError as e:
        print(f"Snapshot error: ConnectionError: {e}")
        return None

    except requests.exceptions.RequestException as e:
        print(f"Snapshot error: RequestException: {type(e).__name__}: {e}")
        return None

    except ValidationError as e:
        print(f"Snapshot validation error: {e}")
        return None

    except Exception as e:
        print(f"Snapshot unexpected error: {type(e).__name__}: {e}")
        return None


# Helpers

def init_order_book(snapshot: SnapshotResponse) -> Dict[str, Dict[float, float]]:
    return {
        "bids": {float(price): float(qty) for price, qty in snapshot.bids},
        "asks": {float(price): float(qty) for price, qty in snapshot.asks},
    }


def extract_depth_payload(msg: Dict[str, Any]) -> Dict[str, Any]:
    return msg.get("data", msg)

"""
    DEBUG / PRESENTATION ONLY.

    This function is used only for visualization.
    Sorting is intentionally used here for readability.
    It is NOT part of the order book synchronization algorithm
    and is not called in the hot data path.

    In production, the order book would be stored
    in a sorted data structure (e.g. tree / heap).
    """
def print_order_book(order_book: Dict[str, Dict[float, float]], top_n: int = 10) -> None:
    bids = sorted(order_book["bids"].items(), key=lambda x: x[0], reverse=True)[:top_n]
    asks = sorted(order_book["asks"].items(), key=lambda x: x[0])[:top_n]

    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

    print("\n" + "=" * 72)
    print(f"ORDER BOOK (top {top_n}) | best_bid={best_bid} best_ask={best_ask} spread={spread}")
    print("-" * 72)
    print(f"{'BIDS (price, qty)':<34} | {'ASKS (price, qty)':<34}")
    print("-" * 72)

    for i in range(max(len(bids), len(asks))):
        left = ""
        right = ""

        if i < len(bids):
            p, q = bids[i]
            left = f"{p:.2f}  {q:.6f}"

        if i < len(asks):
            p, q = asks[i]
            right = f"{p:.2f}  {q:.6f}"

        print(f"{left:<34} | {right:<34}")

    print("=" * 72 + "\n")


def check_buffer_with_snapshot(buffer: List[DepthUpdateData], snapshot_last_id: int) -> List[DepthUpdateData]:
    """
    expected = snapshot_last_id + 1
    ищем первое событие, где U <= expected <= u
    возвращаем буфер начиная с него
    """
    expected = snapshot_last_id + 1

    for i, e in enumerate(buffer):
        if e.u <= snapshot_last_id:
            continue
        if e.U <= expected <= e.u:
            return buffer[i:]

    raise RuntimeError(f"No matching event: snapshot_last_id={snapshot_last_id}, expected={expected}")


def extend_buffer_until_expected(
    ws,
    buffer: List[DepthUpdateData],
    snapshot_last_id: int,
    max_events: int = 20000,
) -> List[DepthUpdateData]:
    expected = snapshot_last_id + 1
    added = 0

    while added < max_events:
        for e in buffer:
            if e.u > snapshot_last_id and e.U <= expected <= e.u:
                return buffer

        msg_data = json.loads(ws.recv())
        payload = extract_depth_payload(msg_data)

        if payload.get("e") != "depthUpdate":
            continue

        try:
            buffer.append(DepthUpdateData(**payload))
            added += 1
        except ValidationError as e:
            print(f"DepthUpdate validation error: {e}")

    raise RuntimeError(f"Could not reach expected={expected} within {max_events} extra WS events")


def apply_depth_update(
    order_book: Dict[str, Dict[float, float]],
    event: DepthUpdateData,
    lastUpdateId: int,
    require_pu: bool,
) -> int:
    if event.u <= lastUpdateId:
        return lastUpdateId

    if require_pu and event.pu != lastUpdateId:
        raise RuntimeError(f"Missed events: event.pu={event.pu} != lastUpdateId={lastUpdateId}")

    for price_str, qty_str in event.b:
        price = float(price_str)
        qty = float(qty_str)
        if qty == 0.0:
            order_book["bids"].pop(price, None)
        else:
            order_book["bids"][price] = qty

    for price_str, qty_str in event.a:
        price = float(price_str)
        qty = float(qty_str)
        if qty == 0.0:
            order_book["asks"].pop(price, None)
        else:
            order_book["asks"][price] = qty

    return event.u

def handle_websocket(
    ws_url: str,
    snapshot_url: str,
    symbol: str,
    snapshot_limit: int = 1000,
    prebuffer_count: int = 50,
    print_every_sec: float = 1.0,
    top_n: int = 10,
):
    print("Подключено к WebSocket...")
    with connect(ws_url) as ws:
        # Буфер до snapshot
        buffer: List[DepthUpdateData] = []

        while len(buffer) < prebuffer_count:
            msg_data = json.loads(ws.recv())
            payload = extract_depth_payload(msg_data)

            if payload.get("e") != "depthUpdate":
                continue

            try:
                buffer.append(DepthUpdateData(**payload))
            except ValidationError as e:
                print(f"DepthUpdate validation error: {e}")

        print(f"Буфер заполнен: {len(buffer)} событий, диапазон: [{buffer[0].U}, {buffer[-1].u}]")

        # Snapshot
        print("Получаем snapshot...")
        snapshot = get_snapshot(snapshot_url, symbol=symbol, size=snapshot_limit)
        if snapshot is None:
            print("Snapshot пуст")
            return

        snapshot_last_id = snapshot.lastUpdateId
        print(f"Snapshot получен: lastUpdateId={snapshot_last_id}")

        # Если snapshot впереди буфера — дочитываем WS, чтобы покрыть expected
        if buffer[-1].u <= snapshot_last_id:
            print("Snapshot впереди буфера")
            try:
                buffer = extend_buffer_until_expected(ws, buffer, snapshot_last_id, max_events=20000)
            except Exception as e:
                print(f"Не удалось дотянуть буфер до snapshot: {e}")
                print("Нужно перезапустить процесс")
                return

        # Находим стартовое событие
        try:
            buffer = check_buffer_with_snapshot(buffer, snapshot_last_id)
        except Exception as e:
            print(f"Не удалось совместить буфер со snapshot: {e}")
            print("Нужно перезапустить процесс с самого начала")
            return

        # Init book
        order_book = init_order_book(snapshot)
        lastUpdateId = snapshot_last_id
        print(f"Локальный ордербук инициализирован. lastUpdateId={lastUpdateId}")
        print(f"Применяем буфер: {len(buffer)} событий")

        # Apply buffer: first without pu, then strict pu
        try:
            first_applied = False

            for event in buffer:
                if event.u <= lastUpdateId:
                    continue

                if not first_applied:
                    expected = lastUpdateId + 1
                    if not (event.U <= expected <= event.u):
                        raise RuntimeError(
                            f"First buffered event doesn't cover expected. "
                            f"event.U={event.U}, event.u={event.u}, expected={expected}"
                        )
                    lastUpdateId = apply_depth_update(order_book, event, lastUpdateId, require_pu=False)
                    first_applied = True
                    continue

                lastUpdateId = apply_depth_update(order_book, event, lastUpdateId, require_pu=True)

        except Exception as e:
            print(f"Ошибка применения буфера: {e}")
            print("Нужно очистить стакан и начать сначала")
            return

        print(f"Sync complete. lastUpdateId={lastUpdateId}")

        print_order_book(order_book, top_n=top_n)

        last_print_ts = time.time()

        while True:
            msg_data = json.loads(ws.recv())
            payload = extract_depth_payload(msg_data)

            if payload.get("e") != "depthUpdate":
                continue

            try:
                event = DepthUpdateData(**payload)
            except ValidationError as e:
                print(f"DepthUpdate error: {e}")
                continue

            try:
                lastUpdateId = apply_depth_update(order_book, event, lastUpdateId, require_pu=True)
            except Exception as e:
                print(f"Stream failed: {e}")
                print("Нужно очистить стакан и начать сначала")
                return

            now = time.time()
            if now - last_print_ts >= print_every_sec:
                print_order_book(order_book, top_n=top_n)
                last_print_ts = now


# Run

if __name__ == "__main__":
    handle_websocket(
        ws_url=WS_URL,
        snapshot_url=REST_SNAPSHOT_URL,
        symbol=SYMBOL,
        snapshot_limit=1000,
        prebuffer_count=50,
        print_every_sec=1.0,
        top_n=10,
    )
