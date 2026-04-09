# Technology Stack

**Analysis Date:** 2026-04-09

## Languages

**Primary:**
- Python 3.11 - entire application (`main.py`)

## Runtime

**Environment:**
- CPython 3.11 (pinned via GitHub Actions `actions/setup-python@v5` with `python-version: "3.11"`)

**Package Manager:**
- pip (upgraded to latest in CI via `python -m pip install --upgrade pip`)
- Lockfile: not present — only `requirements.txt` with minimum version pins

## Frameworks

**Core:**
- None — plain Python script, no web framework

**Build/Dev:**
- No build tool detected (no Makefile, Dockerfile, pyproject.toml, or setup.py)

## Key Dependencies

**Critical:**
- `requests>=2.31` — HTTP client for fetching the REST order book snapshot from Binance Futures API (`main.py:6`, `main.py:50`)
- `pydantic>=1.10` — data validation for REST snapshot and WebSocket depth events via `SnapshotResponse` and `DepthUpdateData` models (`main.py:7`, `main.py:20-34`)
- `websockets>=11.0` — synchronous WebSocket client (`websockets.sync.client.connect`) for the Binance depth stream (`main.py:8`, `main.py:223`)

**Infrastructure:**
- Standard library only beyond the above: `json`, `os`, `sys`, `time`, `typing`

## Configuration

**Environment:**
- `SYMBOL` env var — trading pair symbol, defaults to `btcusdt`. Overridable at runtime via `sys.argv[1]` (CLI arg takes priority over env var). Passed explicitly in GitHub Actions as a workflow input.
- No secrets or API keys required — Binance Futures public endpoints are used without authentication.

**Build:**
- `requirements.txt` — only dependency manifest, no lock file
- `.github/workflows/run-orderbook.yml` — GitHub Actions workflow for CI/scheduled runs

## Tooling

**Linting/Formatting/Type Checking:**
- Not detected — no `.flake8`, `.pylintrc`, `mypy.ini`, `pyproject.toml`, or `.pre-commit-config.yaml` present

## Platform Requirements

**Development:**
- Python 3.11+
- Network access to `fapi.binance.com` and `fstream.binance.com` (geo-restricted; not reachable from GitHub Actions runners)

**Production:**
- Any Linux/macOS host with Python 3.11+ in a Binance-accessible region
- CI: `ubuntu-latest` GitHub Actions runner, job timeout 10 minutes, process runtime capped at 300 seconds via `timeout 300s`

---

*Stack analysis: 2026-04-09*
