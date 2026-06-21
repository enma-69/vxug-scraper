# vxug-scraper

[![CI](https://github.com/YOUR_USER/vxug-scraper/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_USER/vxug-scraper/actions/workflows/ci.yml)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Bulk downloader for the [VX-Underground](https://vx-underground.org) malware research archive.  
Bypasses Cloudflare by driving a real Microsoft Edge session — no proxies, no CAPTCHA solving services.

> **For security research and threat intelligence only.**  
> Never execute samples outside an isolated sandbox environment.

---

## How the Cloudflare bypass works

```
┌─────────────────────────────────────────────────────────────────┐
│  vxdl.py                                                        │
│                                                                 │
│  1. Launch real Edge via pydoll-python (genuine TLS/JA3,        │
│     real V8, real navigator.* APIs — CF sees a real browser)    │
│                                                                 │
│  2. Navigate ONCE to the target section                         │
│     └─ wait for CF challenge to clear (~5-90 s)                 │
│                                                                 │
│  3. Crawl Phoenix LiveView tree by injecting clicks             │
│     into [phx-click] elements — stays in the same              │
│     WebSocket session (no page reloads → no new CF checks)      │
│                                                                 │
│  4. For each discovered file: push to asyncio download queue    │
│     └─ aiohttp workers download immediately (presigned S3       │
│        URLs expire in ~1 h, so crawl + download are pipelined)  │
│                                                                 │
│  5. After all files done: sanitation → classification → report  │
└─────────────────────────────────────────────────────────────────┘
```

| CF Layer | Bypass mechanism |
|---|---|
| TLS fingerprint (JA3) | Real Edge binary — genuine JA3, not Python requests |
| Browser JS checks | Real V8 engine — `navigator.webdriver`, `chrome.*`, `permissions.*` all genuine |
| Bot challenge page | Wait loop polls `document.title` until "checking"/"moment" disappears |
| Rate limiting | Exponential backoff + `Retry-After` header, configurable concurrency |
| Session continuity | One navigation per section; subsequent folder access via LiveView click injection (no page reload) |

---

## Requirements

| Requirement | Notes |
|---|---|
| Python 3.10+ | 3.11+ recommended |
| Microsoft Edge (stable) | [Download here](https://www.microsoft.com/edge) — must be installed, not portable |
| Disk space | ~5 GB for Builders, ~10 GB for Papers, 500 GB+ for full Samples |
| Network | Stable connection — downloads resume automatically on interruption |

---

## Installation

```bash
git clone https://github.com/YOUR_USER/vxug-scraper.git
cd vxug-scraper
pip install -r requirements.txt
```

---

## Usage

### Basic

```bash
# Builders section only (default, ~5 GB)
python vxdl.py

# Multiple sections
python vxdl.py --sections Papers Builders

# Sub-collection (space in name — quote it)
python vxdl.py --sections "Samples/Argus Collection" --max-depth 6

# Test run — only 3 top-level folders, see it work in ~60 s
python vxdl.py --limit 3
```

### Custom output directory

```bash
# Via flag
python vxdl.py --out C:\Research\vxug

# Via environment variable (persists across runs)
set VXUG_OUT=C:\Research\vxug       # Windows
export VXUG_OUT=/data/vxug          # Linux / macOS
python vxdl.py
```

### Long-running / detached (survives terminal close)

```bash
# Launches watchdog → watchdog keeps vxdl.py alive on crash/hang
python launch.py --sections Builders Papers --hours 72

# Full Samples collection (very large — give it days)
python launch.py --sections "Samples/Argus Collection" "Samples/Virusshare Collection" --hours 168 --concurrency 6
```

### Resume an interrupted run

```bash
# Just re-run — already-downloaded URLs are skipped (SQLite dedup)
python vxdl.py --sections Builders
```

### Report only (read-only, safe while downloader is running)

```bash
python report.py                         # auto-finds output/vxdl.db
python report.py --db output/vxdl.db --out output/
```

---

## All CLI flags

### `vxdl.py` — main pipeline

| Flag | Default | Description |
|---|---|---|
| `--sections` | `Builders` | One or more sections: `Builders` `Papers` `Samples` `"Samples/Argus Collection"` `"Samples/Virusshare Collection"` `"Samples/Bazaar Collection"` |
| `--out` | `./output` | Download root directory (`VXUG_OUT` env var overrides) |
| `--concurrency` | `4` | Parallel download workers |
| `--cf-timeout` | `90` | Seconds to wait for Cloudflare to clear per navigation |
| `--max-depth` | `5` | Maximum folder recursion depth (Builders needs 5, Samples needs 6) |
| `--limit` | `0` (all) | Only first N top-level folders — useful for testing |
| `--stage` | full pipeline | Run one stage only: `download` `sanitize` `classify` `report` |
| `--force` | off | Skip environment feasibility check |

### `watchdog.py` — auto-restart daemon

| Flag | Default | Description |
|---|---|---|
| `--sections` | `Builders` | Passed through to `vxdl.py` |
| `--hours` | `48` | Total runtime budget |
| `--hang` | `360` | Seconds of no disk/log growth before declaring a hang |
| `--concurrency` | `4` | Passed through to `vxdl.py` |

### `launch.py` — detached launcher

Same flags as `watchdog.py`. Starts watchdog in a fully detached process that survives terminal close.

### `report.py` — standalone report

| Flag | Default | Description |
|---|---|---|
| `--db` | `./output/vxdl.db` | Path to SQLite database |
| `--out` | `./output` | Directory to write `report.md` and `report.txt` |

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `VXUG_OUT` | `./output` | Output directory for downloads, DB, logs |
| `VXUG_EDGE` | auto-detected | Full path to `msedge.exe` |
| `VXUG_CONCURRENCY` | `4` | Default download worker count |

---

## Pipeline stages

```
Stage 0  Feasibility     Python ≥3.10, packages, Edge binary, disk space
Stage 1  Download        Crawl LiveView tree → discover URLs → download concurrently
                         Resume: already-done URLs skipped via SQLite
Stage 2  Sanitation      Check magic bytes (ZIP, 7z, RAR, PDF, MZ, ELF…) + SHA-256
Stage 3  Classification  Tag platform / impact / malware class from path + extension
Stage 4  Report          Print + save report.txt to output directory
```

---

## Output structure

```
output/
├── vxdl.db               SQLite — all URLs, status, SHA-256, local path, timestamps
├── manifest.csv          Append-only download log (ts, section, folder, file, size, sha256, url)
├── vxdl.log              Full pipeline log
├── report.txt            Collection summary (generated by Stage 4)
├── Builders/
│   ├── NjRat/
│   │   ├── NjRat 0.7d.zip
│   │   └── ...
│   ├── DarkComet/
│   └── ...
├── Papers/
└── Samples/
    ├── Argus Collection/
    ├── Virusshare Collection/
    └── Bazaar Collection/
```

---

## File overview

| File | Purpose |
|---|---|
| [`vxdl.py`](vxdl.py) | Main pipeline — crawl + download + sanitize + classify + report |
| [`watchdog.py`](watchdog.py) | Restart `vxdl.py` on crash or hang, stay within a time budget |
| [`launch.py`](launch.py) | Start the watchdog detached (survives terminal/session close) |
| [`report.py`](report.py) | Standalone report from an existing DB — safe to run mid-download |
| [`requirements.txt`](requirements.txt) | Python dependencies |
| [`pyproject.toml`](pyproject.toml) | Project metadata |

---

## Troubleshooting

**Edge not found**  
Set `VXUG_EDGE` to the full path of `msedge.exe`, or install Edge from [microsoft.com/edge](https://www.microsoft.com/edge).

**Cloudflare never clears**  
Increase `--cf-timeout 120`. If still failing, the IP may be temporarily blocked — wait a few minutes and retry. The watchdog handles this automatically.

**`found 0` files**  
Increase `--max-depth` (default 5, some Samples sub-collections need 6+).

**Download stuck / no progress**  
The watchdog detects hangs (default 360 s of no disk growth) and restarts automatically. Run via `launch.py` for long sessions.

**Resume after crash**  
Re-run the same command — the SQLite database tracks every URL. Already-completed downloads are skipped instantly.

**Windows `[Errno 22]` on filenames**  
`_safe_name()` strips illegal characters (`< > : " | ? *`) and trailing dots/spaces. If you still hit this, check the `vxdl.log` for the offending path.

---

## Legal / ethical notice

This tool is intended for malware research, threat intelligence, and academic study.  
VX-Underground publishes these samples for the security research community.  
Do not execute samples outside an isolated analysis environment (VM with no network, or a dedicated sandbox).  
You are solely responsible for your use of this tool and the content you download.
