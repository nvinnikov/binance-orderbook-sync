# Coding Conventions

**Analysis Date:** 2026-04-09

## Naming Patterns

**Files:**
- Single-file project: `main.py` contains all application logic
- No sub-modules or packages exist

**Functions:**
- `snake_case` for all function names: `get_snapshot`, `init_order_book`, `apply_depth_update`, `handle_websocket`, `print_order_book`, `check_buffer_with_snapshot`, `extend_buffer_until_expected`

**Variables:**
- `snake_case` for local variables: `order_book`, `snapshot_last_id`, `last_print_ts`, `msg_data`
- Exception: `lastUpdateId` uses `camelCase` — mirrors the Binance API field name directly, treated as a domain term rather than a Python identifier

**Constants:**
- `UPPER_SNAKE_CASE` for module-level constants: `SYMBOL`, `REST_SNAPSHOT_URL`, `WS_URL`

**Pydantic Model Classes:**
- `PascalCase`: `SnapshotResponse`, `DepthUpdateData`
- Field names mirror Binance API verbatim (camelCase and single-letter): `lastUpdateId`, `e`, `E`, `s`, `U`, `u`, `pu`, `b`, `a`
- Inline comments clarify field semantics: `# "depthUpdate"`, `# firstUpdateId`, `# bids updates`

## Module Organization

`main.py` is divided into five sections separated by comment headers:

```python
#Env            # Module-level constants and environment variables (lines 11-15)
#Models         # Pydantic model definitions (lines 18-34)
# REST Snapshot # REST API fetch logic: get_snapshot() (lines 37-80)
# Helpers       # Pure helper functions and order book logic (lines 83-331)
# Run           # Entry point: if __name__ == "__main__" block (lines 333-344)
```

Note: spacing after `#` is inconsistent (`#Env`, `#Models` vs `# REST Snapshot`, `# Helpers`, `# Run`).

## Type Hints

Type hints are used throughout. All function signatures are fully annotated:

```python
def get_snapshot(url: str, symbol: str, size: int = 1000) -> SnapshotResponse | None:
def init_order_book(snapshot: SnapshotResponse) -> Dict[str, Dict[float, float]]:
def apply_depth_update(
    order_book: Dict[str, Dict[float, float]],
    event: DepthUpdateData,
    lastUpdateId: int,
    require_pu: bool,
) -> int:
```

Imports use `from typing import Any, Dict, List` (Python 3.8-compatible style) rather than the newer built-in generics (`dict`, `list`). The union return type `SnapshotResponse | None` uses the Python 3.10+ `X | Y` syntax — mixing both styles.

## Docstring Style

Docstrings are used sparingly. Only `check_buffer_with_snapshot` and `print_order_book` have them.

`check_buffer_with_snapshot` uses a plain multi-line string with inline Russian and a pseudocode description:
```python
"""
expected = snapshot_last_id + 1
ищем первое событие, где U <= expected <= u
возвращаем буфер начиная с него
"""
```

`print_order_book` has a free-form block comment (misplaced — appears before the `def` line rather than inside the function body):
```python
"""
DEBUG / PRESENTATION ONLY.

This function is used only for visualization.
Sorting is intentionally used here for readability.
...
"""
def print_order_book(...):
```

No functions use PEP 257 / Google / NumPy docstring conventions. Most functions are undocumented.

## Error Handling Patterns

Errors are caught and logged via `print()` — no logging framework is used.

**`get_snapshot` catches granular exception types:**
```python
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
```

**`handle_websocket` uses broad `except Exception` with early return on failure:**
```python
except Exception as e:
    print(f"Не удалось дотянуть буфер до snapshot: {e}")
    print("Нужно перезапустить процесс")
    return
```

**Pydantic `ValidationError` is caught inline during buffer accumulation to skip invalid messages without crashing:**
```python
try:
    buffer.append(DepthUpdateData(**payload))
except ValidationError as e:
    print(f"DepthUpdate validation error: {e}")
```

**`RuntimeError` is raised explicitly for continuity violations:**
```python
raise RuntimeError(f"Missed events: event.pu={event.pu} != lastUpdateId={lastUpdateId}")
raise RuntimeError(f"No matching event: snapshot_last_id={snapshot_last_id}, expected={expected}")
```

## Language Mix

Print statements and inline comments appear in both English and Russian. Russian is used for operational/user-facing messages in `handle_websocket`:

- `"Подключено к WebSocket..."` — "Connected to WebSocket..."
- `"Буфер заполнен: {len(buffer)} событий, диапазон: [...]"` — buffer filled status
- `"Получаем snapshot..."` — "Fetching snapshot..."
- `"Snapshot пуст"` — "Snapshot is empty"
- `"Нужно перезапустить процесс"` — "Process needs to restart"
- `"Ошибка применения буфера: {e}"` — "Buffer application error"

English is used for error messages in `get_snapshot`, the final sync confirmation (`"Sync complete."`), and all code-level comments about algorithm correctness.

## Comments

Inline comments annotate algorithm-critical conditions:
```python
# Binance Futures API limit: 5..1000
# Буфер до snapshot
# Если snapshot впереди буфера — дочитываем WS, чтобы покрыть expected
# Apply buffer: first without pu, then strict pu
```

Section comments (`# REST Snapshot`, `# Helpers`, `# Run`) serve as structural separators.

## Output / Logging

No logging framework (`logging` module) is used. All output goes to stdout via `print()`. There is no log level control, no timestamps on log lines, and no structured logging. The CI workflow captures stdout via `tee orderbook.log`.

---

*Convention analysis: 2026-04-09*
