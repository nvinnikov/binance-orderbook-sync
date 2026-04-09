# Codebase Concerns

**Analysis Date:** 2026-04-09

## Tech Debt

**No automated reconnection on failure:**
- Issue: Every error path in `handle_websocket` prints a message and returns, terminating the process. There is no retry loop or reconnection logic. A single missed WS event, a snapshot HTTP failure, or a `pu` sequence gap causes a silent exit.
- Files: `main.py` lines 244-268, 296-299, 320-325
- Impact: The process must be restarted manually every time any recoverable error occurs. In production this would cause extended downtime and missed data.
- Fix approach: Wrap `handle_websocket` in a retry loop in `__main__` with exponential backoff; replace `return` on recoverable errors with `raise` so the loop can catch and restart.
- Severity: HIGH

**No retry or rate-limit handling for REST snapshot:**
- Issue: `get_snapshot` makes a single HTTP request with a 10-second timeout. HTTP 429 (rate limited) or 503 (temporary unavailability) are treated identically to a permanent failure: the function returns `None` and the caller exits.
- Files: `main.py` lines 49-80, 243-246
- Impact: A transient server hiccup during snapshot fetch terminates the entire synchronization process.
- Fix approach: Add retry with backoff for 429/5xx responses; respect the `Retry-After` header Binance sends on rate-limit responses.
- Severity: HIGH

**Single-threaded blocking WebSocket loop — no async:**
- Issue: The code uses `websockets.sync.client.connect`, which is a synchronous blocking API. The receive loop (`ws.recv()`) blocks indefinitely. Any CPU-intensive operation (e.g. sorting, printing) inside the loop delays message consumption, which can cause WS buffer overflow or missed events under high-frequency update bursts.
- Files: `main.py` lines 223, 307-330
- Impact: Under peak market activity the main loop falls behind real-time, leading to sequence gaps that trigger `RuntimeError` and process exit.
- Fix approach: Switch to `websockets.asyncio.client` and use `asyncio`; or move printing/heavy computation to a background thread.
- Severity: MEDIUM

**Order book stored as unsorted plain `dict`:**
- Issue: `init_order_book` returns `{"bids": {float: float}, "asks": {float: float}}`. Python dicts preserve insertion order, not price order. The comment in `print_order_book` (lines 95-105) explicitly acknowledges this is not production-grade and that a sorted structure (tree/heap) should be used instead.
- Files: `main.py` lines 85-89, 95-105
- Impact: Fetching best bid/ask requires a full `sorted()` scan on every print cycle — O(n) instead of O(log n). For a 1000-level book at 1 Hz this is acceptable for display, but any consumer needing best-price in real time would pay this cost on every query.
- Fix approach: Use `sortedcontainers.SortedDict` or a red-black tree for O(log n) insert/delete/min/max.
- Severity: MEDIUM

**`float` as dict key for prices — potential precision issues:**
- Issue: `init_order_book` and `apply_depth_update` both call `float(price_str)` and use the result as a dict key. IEEE 754 float representation of decimal price strings (e.g. `"0.1"`) can produce keys that do not round-trip exactly, causing phantom duplicate entries or missed removals when the same price level arrives as slightly different float representations.
- Files: `main.py` lines 87-88, 195-209
- Impact: Silent order book corruption — a level that should be removed (`qty == 0.0`) might not match its stored key, leaving a stale entry. This is an algorithmic correctness risk.
- Fix approach: Use `Decimal` for price keys (from Python's `decimal` module) or keep prices as the original string representation.
- Severity: HIGH

**`max_events=20000` hardcoded limit in `extend_buffer_until_expected`:**
- Issue: The function signature has `max_events: int = 20000` as a default, but the only call site also hard-codes `20000` as a positional argument (`main.py` line 255), making the parameter effectively a magic constant. There is no configuration mechanism to adjust this.
- Files: `main.py` lines 154-180, 255
- Impact: On a slow connection or under a very large snapshot ID gap, 20000 events may not be enough, causing an immediate `RuntimeError` and process exit. Conversely on a fast feed, waiting for 20000 events before giving up wastes time.
- Fix approach: Make the limit configurable via an environment variable or CLI argument; document the reasoning for the chosen value.
- Severity: LOW

**Mixed language in log messages (Russian + English):**
- Issue: Approximately half the `print` statements are in Russian (e.g. `"Подключено к WebSocket..."`, `"Нужно перезапустить процесс"`) and the other half in English (e.g. `"Sync complete."`, `"Stream failed:"`). There is no consistent language policy.
- Files: `main.py` lines 222, 239, 242, 246, 249, 257-258, 265-266, 272-274, 297-298, 301, 323-324
- Impact: Log output is difficult to parse for non-Russian readers and makes automated log monitoring (regex, alerting) harder because the same semantic event may appear in either language.
- Fix approach: Standardize all user-facing strings to English; move any Russian inline comments to English as well.
- Severity: LOW

**Pydantic v1 vs v2 compatibility:**
- Issue: `requirements.txt` pins `pydantic>=1.10`, which allows both v1 and v2. The code uses `from pydantic import BaseModel, ValidationError` with no version guard. Pydantic v2 introduced breaking changes to field behavior, error formats, and model construction. Installing v2 (the current default from PyPI) will likely work for simple models here, but any future use of v1-only APIs would silently differ.
- Files: `requirements.txt` line 2, `main.py` lines 7, 20-34, 56, 74, 175, 234, 315
- Impact: Dependency resolution on a fresh install pulls in Pydantic v2. If the project is ever extended with v1-specific patterns (validators, `__fields__`, etc.) it will fail at runtime on environments that resolved v2.
- Fix approach: Pin `pydantic>=2.0` explicitly and update to v2 idioms, or pin `pydantic>=1.10,<2.0` if v1 is intentional.
- Severity: MEDIUM

## Known Bugs

**Sequence gap detection exits instead of resyncing:**
- Symptoms: When `apply_depth_update` detects `event.pu != lastUpdateId` it raises `RuntimeError`. The caller catches it, prints a Russian message, and `return`s from `handle_websocket`. The process then terminates because `__main__` makes no further calls.
- Files: `main.py` lines 192-193, 296-299, 320-325, 335-344
- Trigger: Any brief network interruption causing a missed WS message.
- Workaround: Manual restart of the process.

## Security Considerations

**Binance API geo-restriction (HTTP 451):**
- Risk: GitHub Actions runners run on Azure data centers. Binance Futures API (`fapi.binance.com`) returns HTTP 451 (Unavailable For Legal Reasons) from US-based IPs, blocking snapshot fetches entirely.
- Files: `.github/workflows/run-orderbook.yml`, `main.py` line 51-54
- Current mitigation: None. The workflow will silently fail at snapshot fetch when run on a US-based runner.
- Recommendations: Add a non-US runner label, use a self-hosted runner, or add explicit detection of HTTP 451 with a clear error message distinguishing geo-restriction from other failures.
- Severity: HIGH (for CI workflow)

## Performance Bottlenecks

**O(n) sort on every print cycle:**
- Problem: `print_order_book` calls `sorted()` on both bids and asks dicts on every invocation (default: every 1 second). For a 1000-level book this is 2000 comparisons per print.
- Files: `main.py` lines 107-108
- Cause: Underlying storage is an unsorted dict; see "Order book stored as unsorted plain dict" above.
- Improvement path: Use a sorted container so best bid/ask are O(1) lookups and top-N is O(N).

## Test Coverage Gaps

**No tests exist:**
- What's not tested: The entire synchronization algorithm — buffer alignment (`check_buffer_with_snapshot`), sequence gap detection (`apply_depth_update`), buffer extension (`extend_buffer_until_expected`), snapshot parsing, and the main WS loop.
- Files: `main.py` (all logic), no `test_*.py` or `*_test.py` files present in the repository.
- Risk: Algorithm correctness relies entirely on manual observation of live output. A regression in any of the sequence-matching logic would not be caught before deployment.
- Priority: HIGH — the synchronization algorithm has multiple subtle invariants (U/u/pu relationships, first-event relaxed check) that are ideal unit test candidates with mock WS data.

---

*Concerns audit: 2026-04-09*
