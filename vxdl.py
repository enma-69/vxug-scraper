#!/usr/bin/env python3
"""
vx-underground downloader — single-session click navigation + concurrent download.

Why this design:
  * Presigned S3 URLs expire in 3600 s → must download while crawling (not after).
  * Re-navigating to a section per folder triggers a fresh Cloudflare challenge
    (~8 s) and gets rate-limited fast.  So we navigate ONCE, then move through the
    Phoenix-LiveView tree by clicking (no page reloads, one persistent WebSocket).
  * LiveView fills phx-click paths progressively (~9 s just after CF clears), so
    "ready" means "many populated paths", not just "WS connected".

Install:
    pip install -r requirements.txt

Usage:
    python vxdl.py                                     # Builders only
    python vxdl.py --sections Papers Builders          # multiple sections
    python vxdl.py --sections "Samples/Argus Collection" --concurrency 4 --max-depth 6
    python vxdl.py --stage report                      # re-run report only

Environment variables (override defaults):
    VXUG_OUT        output directory          (default: ./output)
    VXUG_EDGE       path to msedge.exe        (auto-detected if unset)
    VXUG_CONCURRENCY  download threads        (default: 4)
"""
from __future__ import annotations

import argparse, asyncio, csv, hashlib, json, logging
import os, random, sqlite3, sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse, quote

import aiofiles
import aiohttp
from rich.console import Console
from rich.progress import (
    BarColumn, DownloadColumn, Progress, TextColumn,
    TimeElapsedColumn, TransferSpeedColumn,
)

from pydoll.browser import Edge
from pydoll.browser.options import ChromiumOptions

# ── Config ────────────────────────────────────────────────────────────────────

def _find_edge() -> str:
    """Return the first msedge.exe path that actually exists."""
    candidates = [
        os.environ.get("VXUG_EDGE", ""),
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        # Linux / CI
        "/usr/bin/microsoft-edge",
        "/usr/bin/microsoft-edge-stable",
        # macOS
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    ]
    for c in candidates:
        if c and Path(c).exists():
            return c
    return candidates[1]  # return the Windows default even if missing so error is clear

EDGE_PATH  = _find_edge()
BASE_URL   = "https://vx-underground.org"
OUTPUT_DIR = Path(os.environ.get("VXUG_OUT", Path(__file__).parent / "output"))
DB_NAME    = "vxdl.db"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0"
)
CHUNK      = 65_536
RETRIES    = 4
RETRY_BASE = 4.0
RL_BACKOFF = 60.0
SENTINEL   = None  # queue EOF marker

# Force UTF-8 on Windows so CJK filenames / box-drawing never crash on cp1252.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

CONSOLE = Console(highlight=False, legacy_windows=False, safe_box=True)


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_log(path: Path) -> logging.Logger:
    log = logging.getLogger("vxdl")
    if log.handlers:
        return log
    log.setLevel(logging.DEBUG)
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
    log.addHandler(fh)
    return log


# ── SQLite ────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    url        TEXT PRIMARY KEY,
    folder     TEXT,
    filename   TEXT,
    section    TEXT,
    local_path TEXT    DEFAULT '',
    status     TEXT    DEFAULT 'queued',
    size_bytes INTEGER DEFAULT 0,
    sha256     TEXT    DEFAULT '',
    dl_at      TEXT    DEFAULT '',
    error      TEXT    DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_status ON files(status);
"""

def open_db(path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(path), check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    con.commit()
    return con

def db_done_set(con: sqlite3.Connection) -> set:
    return {r["url"] for r in con.execute("SELECT url FROM files WHERE status='done'")}

def db_add(con: sqlite3.Connection, e: dict) -> None:
    con.execute(
        "INSERT OR IGNORE INTO files (url,folder,filename,section) VALUES (?,?,?,?)",
        (e["url"], e["folder"], e["filename"], e["section"]),
    )
    con.commit()

def db_done(con: sqlite3.Connection, url: str, local: str, size: int, sha: str) -> None:
    con.execute(
        "UPDATE files SET status='done',local_path=?,size_bytes=?,sha256=?,dl_at=? WHERE url=?",
        (local, size, sha, datetime.now(timezone.utc).isoformat(), url),
    )
    con.commit()

def db_fail(con: sqlite3.Connection, url: str, err: str) -> None:
    con.execute("UPDATE files SET status='failed',error=? WHERE url=?", (err, url))
    con.commit()

def _has_col(con: sqlite3.Connection, name: str) -> bool:
    return any(c[1] == name for c in con.execute("PRAGMA table_info(files)"))


# ── pydoll helpers ────────────────────────────────────────────────────────────

def _jsv(raw) -> str:
    if isinstance(raw, dict):
        return raw.get("result", {}).get("result", {}).get("value", "") or ""
    return str(raw) if raw is not None else ""

async def _js(page, script: str) -> str:
    return _jsv(await page.execute_script(script))

async def _delay(lo: float = 0.6, hi: float = 1.4) -> None:
    await asyncio.sleep(random.uniform(lo, hi))

_ILLEGAL = '<>:"|?*'
def _safe_name(name: str) -> str:
    """Strip characters Windows forbids in filenames (fixes [Errno 22])."""
    out = "".join("_" if c in _ILLEGAL else c for c in name)
    out = out.rstrip(" .")  # trailing space / dot also forbidden on Windows
    return out or "unnamed"

async def _all_paths(page) -> list[str]:
    """All phx-click path values currently visible on the page."""
    raw = await _js(page, """
    const out=[];
    for(const el of document.querySelectorAll('[phx-click]')){
        try{
            const v=JSON.parse(el.getAttribute('phx-click'))[0][1].value.value;
            if(v&&v.length>0) out.push(v);
        }catch(e){}
    }
    return JSON.stringify(out);
    """)
    try:
        return json.loads(raw) if raw else []
    except Exception:
        return []

async def _children(page, parent: str) -> list[str]:
    """Direct child folders of *parent* — exactly one level deeper."""
    paths = await _all_paths(page)
    pd = parent.count("/")
    seen: set[str] = set()
    out: list[str] = []
    for p in paths:
        if p.startswith(parent) and p != parent and p.count("/") == pd + 1:
            if p not in seen:
                seen.add(p)
                out.append(p)
    return out

async def _file_links(page) -> list[str]:
    """All downloadable hrefs visible on the page (S3 presigned + common extensions)."""
    raw = await _js(page, r"""
    const out=[];
    for(const a of document.querySelectorAll('a[href]')){
        const h=a.href;
        if(h.includes('backblazeb2.com')||h.includes('s3.us-east')||
           /\.(zip|7z|rar|tar|gz|bz2|xz|exe|dll|sys|bin|msi|bat|cmd|vbs|ps1|py|rb|sh
              |c|cpp|cs|asm|java|pdf|doc|docx|txt|apk|elf)(\?|$)/i.test(h))
            out.push(h);
    }
    return JSON.stringify(out);
    """)
    try:
        return list(dict.fromkeys(json.loads(raw) if raw else []))
    except Exception:
        return []

async def _click(page, path: str) -> bool:
    raw = await _js(page, f"""
    const t=Array.from(document.querySelectorAll('[phx-click]')).find(d=>{{
        try{{return JSON.parse(d.getAttribute('phx-click'))[0][1].value.value==={json.dumps(path)};}}
        catch(e){{return false;}}
    }});
    if(t){{t.click();return true;}}return false;
    """)
    return raw is True or str(raw).lower() == "true"

async def _wait_cf(page, timeout_s: int = 90, log=None) -> bool:
    """Block until Cloudflare challenge clears (title no longer says 'checking'/'moment')."""
    for i in range(timeout_s):
        await asyncio.sleep(1)
        t = await _js(page, "return document.title;")
        if t and "moment" not in t.lower() and "checking" not in t.lower():
            if log:
                log.info("[CF] cleared after %ds — title=%r", i + 1, t)
            return True
    return False

async def _wait_paths(page, minpaths: int, timeout_s: int = 30, log=None) -> int:
    """Wait until >= *minpaths* phx-click elements carry a populated path value."""
    for _ in range(timeout_s * 2):
        await asyncio.sleep(0.5)
        n = int(await _js(page, """
        let c=0;
        for(const el of document.querySelectorAll('[phx-click]')){
            try{const v=JSON.parse(el.getAttribute('phx-click'))[0][1].value.value;
                if(v&&v.length>0)c++;}catch(e){}
        }
        return String(c);
        """) or 0)
        if n >= minpaths:
            return n
    return 0

async def _wait_ready_any(page, section: str, timeout_s: int = 40, log=None) -> tuple[int, int]:
    """For sub-collections whose root may show FILES directly (no intermediate folders).
    Returns (n_child_folders, n_file_links)."""
    for i in range(timeout_s):
        await asyncio.sleep(1)
        t = await _js(page, "return document.title;")
        if "moment" in t.lower():
            continue
        kids  = await _children(page, f"{section}/")
        files = await _file_links(page)
        if kids or files:
            if log:
                log.info("[ready] %s: %d folders, %d files at %ds", section, len(kids), len(files), i + 1)
            return len(kids), len(files)
    if log:
        log.warning("[ready] %s: nothing after %ds", section, timeout_s)
    return 0, 0

async def _wait_content(page, parent: str, timeout_s: int = 12) -> tuple[int, int]:
    """After clicking into *parent*, wait for its children or file links to appear."""
    for _ in range(timeout_s * 2):
        await asyncio.sleep(0.5)
        kids  = await _children(page, parent)
        files = await _file_links(page)
        if kids or files:
            return len(kids), len(files)
    return 0, 0


# ── Crawl (click-navigation, single LiveView session) ─────────────────────────

async def crawl(page, section: str, path: str, depth: int, max_depth: int,
                queue: asyncio.Queue, con: sqlite3.Connection, done: set,
                log, stats: dict) -> None:
    """Precondition: the browser is currently displaying *path*'s contents."""
    files = await _file_links(page)
    new = 0
    for href in files:
        if href in done:
            continue
        fname = unquote(Path(urlparse(href).path).name)
        e = {"section": section, "folder": path, "filename": fname, "url": href}
        db_add(con, e)
        await queue.put(e)
        done.add(href)
        new += 1
    if new:
        stats["found"] += new
        log.info("%s%s  +%d files", "  " * depth, path, new)
        CONSOLE.log(f"  [crawl] {path}  +{new} files  (total found {stats['found']})")

    if depth >= max_depth:
        return

    kids = await _children(page, path)
    for kid in kids:
        if not await _click(page, kid):
            log.warning("%scannot click %s", "  " * depth, kid)
            continue
        await _wait_content(page, kid)
        await _delay()
        await crawl(page, section, kid, depth + 1, max_depth, queue, con, done, log, stats)
        # navigate back to parent
        if not await _click(page, path):
            log.warning("%scannot return to %s — state lost", "  " * depth, path)
            return
        await _wait_content(page, path)
        await _delay()


# ── Download workers ───────────────────────────────────────────────────────────

async def dl_worker(queue: asyncio.Queue, session: aiohttp.ClientSession,
                    out_dir: Path, con: sqlite3.Connection,
                    progress, overall, csv_wr, log, stats: dict) -> None:
    while True:
        e = await queue.get()
        if e is SENTINEL:
            queue.task_done()
            return
        url, folder, fname, section = e["url"], e["folder"], e["filename"], e["section"]
        rel  = Path(*[_safe_name(p) for p in folder.split("/")]) if "/" in folder else Path(_safe_name(folder))
        ddir = out_dir / rel
        ddir.mkdir(parents=True, exist_ok=True)
        dest = ddir / _safe_name(fname)
        tid  = progress.add_task(f"{fname[:38]}", total=None)
        ok, nb, sha, err = await dl_file(session, url, dest, progress, tid, overall, log)
        progress.remove_task(tid)
        mib = nb / 1_048_576
        if ok:
            stats["done"] += 1
            stats["mib"]  += mib
            db_done(con, url, str(dest), nb, sha)
            csv_wr.writerow({
                "ts": datetime.now(timezone.utc).isoformat(),
                "section": section, "folder": folder, "filename": fname,
                "size_mb": f"{mib:.2f}", "sha256": sha, "url": url,
            })
            CONSOLE.log(f"  [OK]   {fname[:50]}  {mib:.1f} MB")
            log.info("[OK] %s %.2f MiB", fname, mib)
        else:
            stats["fail"] += 1
            db_fail(con, url, err)
            CONSOLE.log(f"  [FAIL] {fname[:50]}  {err}")
            log.warning("[FAIL] %s %s", fname, err)
        progress.update(overall, completed=stats["done"] + stats["fail"],
                        description=f"{stats['done']} done / {stats['found']} found")
        queue.task_done()


async def dl_file(session: aiohttp.ClientSession, url: str, dest: Path,
                  progress, tid, overall, log) -> tuple[bool, int, str, str]:
    fname = dest.name
    for attempt in range(1, RETRIES + 1):
        try:
            hdrs, mode, resume = {}, "wb", 0
            if dest.exists():
                resume = dest.stat().st_size
                if resume:
                    hdrs["Range"] = f"bytes={resume}-"
                    mode = "ab"
            timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_read=120)
            async with session.get(url, headers=hdrs, timeout=timeout) as r:
                if r.status == 416:                        # already complete
                    sz = dest.stat().st_size
                    progress.update(tid, completed=sz, total=sz)
                    return True, sz, hashlib.sha256(dest.read_bytes()).hexdigest(), ""
                if r.status in (429, 503):
                    wait = float(r.headers.get("Retry-After", RL_BACKOFF))
                    log.warning("[%s] rate-limited — sleeping %.0fs", fname, wait)
                    await asyncio.sleep(wait)
                    continue
                if r.status not in (200, 206):
                    await asyncio.sleep(RETRY_BASE * attempt)
                    continue
                if r.status == 200 and resume:             # server ignored Range
                    mode, resume = "wb", 0
                cl = r.headers.get("Content-Length")
                if cl:
                    progress.update(tid, total=resume + int(cl))
                sha = hashlib.sha256()
                written = 0
                async with aiofiles.open(dest, mode) as f:
                    async for chunk in r.content.iter_chunked(CHUNK):
                        await f.write(chunk)
                        sha.update(chunk)
                        written += len(chunk)
                        progress.update(tid, advance=len(chunk))
                        progress.update(overall, advance=len(chunk))
                return True, resume + written, sha.hexdigest(), ""
        except asyncio.TimeoutError:
            log.warning("[%s] timeout attempt %d", fname, attempt)
        except Exception as ex:
            log.warning("[%s] error attempt %d: %s", fname, attempt, ex)
        if attempt < RETRIES:
            await asyncio.sleep(RETRY_BASE * attempt + random.uniform(0, 2))
    return False, 0, "", "max retries"


# ── Root-reset helper ──────────────────────────────────────────────────────────

async def _goto_root(page, sec_url: str, section: str, cf_timeout: int, log) -> bool:
    """Return browser to the section root (shows all top folders).
    Tries a fast click-back first; falls back to full re-navigation if state is corrupt."""
    if await _click(page, f"{section}/"):
        n = await _wait_paths(page, minpaths=100, timeout_s=8)
        if n >= 100:
            return True
    log.info("[root] click-back insufficient — re-navigating to %s", sec_url)
    try:
        await asyncio.wait_for(page.go_to(sec_url, timeout=45), timeout=50)
    except Exception as ex:
        log.warning("[root] re-nav error: %s", ex)
    await _wait_cf(page, cf_timeout, log)
    n = await _wait_paths(page, minpaths=100, timeout_s=30, log=log)
    return n >= 100


# ═══ STAGE: SANITATION ════════════════════════════════════════════════════════

MAGIC: dict[str, bytes] = {
    ".zip": b"PK\x03\x04",
    ".7z":  b"7z\xbc\xaf'\x1c",
    ".rar": b"Rar!",
    ".gz":  b"\x1f\x8b",
    ".pdf": b"%PDF",
    ".exe": b"MZ",
    ".dll": b"MZ",
    ".elf": b"\x7fELF",
    ".apk": b"PK\x03\x04",
}

def _check_magic(path: Path) -> tuple[bool, str]:
    try:
        if path.stat().st_size == 0:
            return False, "zero-byte"
        exp = MAGIC.get(path.suffix.lower())
        if exp:
            with path.open("rb") as f:
                head = f.read(len(exp))
            if head != exp:
                return False, f"bad-magic {head.hex()[:12]}"
        return True, "ok"
    except Exception as e:
        return False, str(e)[:40]

def stage_sanitize(con: sqlite3.Connection, log) -> tuple[int, int]:
    if not _has_col(con, "san"):
        con.execute("ALTER TABLE files ADD COLUMN san TEXT DEFAULT ''")
        con.commit()
    rows = con.execute(
        "SELECT url, local_path FROM files WHERE status='done'"
    ).fetchall()
    good = bad = 0
    with Progress(TextColumn("[sanitize] {task.description}"),
                  BarColumn(bar_width=30), console=CONSOLE) as p:
        t = p.add_task("checking", total=len(rows))
        for r in rows:
            local = Path(r["local_path"])
            ok, note = _check_magic(local) if local.exists() else (False, "missing")
            con.execute("UPDATE files SET san=? WHERE url=?",
                        ("ok" if ok else note, r["url"]))
            good += ok; bad += not ok
            p.advance(t)
        con.commit()
    CONSOLE.print(f"  [sanitize] valid={good}  bad={bad}")
    log.info("Sanitize: good=%d bad=%d", good, bad)
    return good, bad


# ═══ STAGE: CLASSIFICATION ════════════════════════════════════════════════════

PLATFORM: dict[str, str] = {
    "exe": "Windows", "dll": "Windows", "sys": "Windows", "msi": "Windows",
    "bat": "Windows", "cmd": "Windows", "ps1": "Windows", "vbs": "Windows", "hta": "Windows",
    "elf": "Linux",   "sh": "Linux",    "apk": "Android",
    "dmg": "macOS",
    "py":  "Script",  "rb": "Script",   "php": "Script",  "jar": "Java",
    "pdf": "Document","doc": "Document","docx": "Document","txt": "Document",
    "zip": "Archive", "7z": "Archive",  "rar": "Archive",
}

CLASS_RULES: list[tuple[str, list[str]]] = [
    ("Ransomware",  ["ransom","locker","wannacry","ryuk","conti","lockbit","babuk","phobos"]),
    ("Worm",        ["worm","mozi","spreader"]),
    ("Rootkit",     ["rootkit","kernel","ring0","bootkit"]),
    ("RAT",         ["rat","remote","njrat","remcos","nanocore","quasar","darkcomet",
                     "gh0st","orcus","asyncrat","bitrat","bifrost","cybergate","blackshades"]),
    ("Botnet",      ["bot","mirai","gafgyt","ddos","emotet","flood","andromeda","citadel"]),
    ("Stealer",     ["steal","redline","vidar","raccoon","formbook","agenttesla","azorult",
                     "lokibot","grabber","clipper","keylog","logger","collector"]),
    ("Backdoor",    ["backdoor","shell","c2","implant","backorifice","bandook","beastdoor"]),
    ("Loader",      ["loader","amadey","smokeloader","dropper","stub","darkgate"]),
    ("Banker",      ["banker","banking","zeus","dridex","trickbot","caberp"]),
    ("Exploit",     ["exploit","cve-","poc","shellcode"]),
    ("Cryptominer", ["miner","xmrig","monero","chminer"]),
    ("Spyware",     ["spy","stalker","monitor"]),
    ("Trojan",      ["trojan","andro"]),
]

IMPACT: dict[str, str] = {
    "Ransomware":  "Critical",
    "Worm":        "Critical",
    "Rootkit":     "Critical",
    "RAT":         "High",
    "Botnet":      "High",
    "Backdoor":    "High",
    "Banker":      "High",
    "Stealer":     "High",
    "Loader":      "Medium",
    "Exploit":     "Medium",
    "Trojan":      "Medium",
    "Spyware":     "Medium",
    "Cryptominer": "Low",
    "Builder":     "Medium",
    "Research":    "Info",
    "Unknown":     "Unknown",
}

def _classify(section: str, folder: str, filename: str) -> tuple[str, str, str]:
    ext  = Path(filename).suffix.lstrip(".").lower()
    plat = PLATFORM.get(ext, "Unknown")
    if section == "Papers":
        return (plat or "Document"), "Info", "Research"
    key = (folder + " " + filename).lower()
    cls = "Unknown"
    for name, kws in CLASS_RULES:
        if any(k in key for k in kws):
            cls = name
            break
    if cls == "Unknown" and section == "Builders":
        cls = "Builder"
    return plat, IMPACT.get(cls, "Unknown"), cls

def stage_classify(con: sqlite3.Connection, log) -> int:
    for col in ("platform", "impact", "mclass"):
        if not _has_col(con, col):
            con.execute(f"ALTER TABLE files ADD COLUMN {col} TEXT DEFAULT ''")
    con.commit()
    rows = con.execute(
        "SELECT url, section, folder, filename FROM files WHERE status='done'"
    ).fetchall()
    with Progress(TextColumn("[classify] {task.description}"),
                  BarColumn(bar_width=30), console=CONSOLE) as p:
        t = p.add_task("tagging", total=len(rows))
        for r in rows:
            plat, imp, cls = _classify(r["section"], r["folder"], r["filename"])
            con.execute("UPDATE files SET platform=?,impact=?,mclass=? WHERE url=?",
                        (plat, imp, cls, r["url"]))
            p.advance(t)
        con.commit()
    CONSOLE.print(f"  [classify] tagged {len(rows)} files")
    log.info("Classify: %d files", len(rows))
    return len(rows)


# ═══ STAGE: REPORT ════════════════════════════════════════════════════════════

def stage_report(con: sqlite3.Connection, out_dir: Path, log) -> None:
    from rich.table import Table
    from rich import box as _box

    total = con.execute("SELECT COUNT(*) FROM files WHERE status='done'").fetchone()[0]
    gb    = con.execute(
        "SELECT COALESCE(SUM(size_bytes),0)/1073741824.0 FROM files WHERE status='done'"
    ).fetchone()[0]
    fail  = con.execute("SELECT COUNT(*) FROM files WHERE status='failed'").fetchone()[0]

    summ = Table(box=_box.ROUNDED, title="Collection Summary")
    summ.add_column("Metric", style="bold cyan", justify="right")
    summ.add_column("Value")
    summ.add_row("Downloaded", f"[green]{total}[/]")
    summ.add_row("Failed",     f"[red]{fail}[/]" if fail else "0")
    summ.add_row("Total size", f"{gb:.2f} GB")
    if _has_col(con, "san"):
        sok = con.execute("SELECT COUNT(*) FROM files WHERE san='ok'").fetchone()[0]
        summ.add_row("Sanitized OK", str(sok))
    CONSOLE.print(summ)

    if _has_col(con, "mclass"):
        ct = Table(box=_box.SIMPLE, title="By Class / Impact / Platform")
        ct.add_column("Class",    style="bold")
        ct.add_column("Impact",   justify="center")
        ct.add_column("Platform", style="dim")
        ct.add_column("Count",    justify="right")
        for r in con.execute(
            "SELECT mclass,impact,platform,COUNT(*) n FROM files "
            "WHERE status='done' GROUP BY mclass,impact,platform ORDER BY n DESC LIMIT 40"
        ):
            ct.add_row(r[0] or "?", r[1] or "?", r[2] or "?", str(r[3]))
        CONSOLE.print(ct)

    rep = out_dir / "report.txt"
    with CONSOLE.capture() as cap:
        CONSOLE.print(summ)
        if _has_col(con, "mclass"):
            CONSOLE.print(ct)
    rep.write_text(cap.get(), encoding="utf-8")
    CONSOLE.print(f"  Report saved: {rep}")
    log.info("Report: total=%d gb=%.2f fail=%d", total, gb, fail)


# ── Stage 0: Feasibility check ────────────────────────────────────────────────

def stage_env(out_dir: Path, sections: list[str], log) -> bool:
    from rich.table import Table
    from rich import box as _box
    import shutil as _sh

    ok = True
    t = Table(box=_box.SIMPLE, show_header=False)
    t.add_column("Item",   style="bold cyan")
    t.add_column("Status")
    t.add_column("Detail", style="dim")

    v = sys.version_info
    t.add_row("Python", "OK" if v >= (3, 10) else "FAIL", f"{v.major}.{v.minor}.{v.micro}")
    if v < (3, 10):
        ok = False

    for pkg in ("pydoll", "aiohttp", "aiofiles", "rich"):
        try:
            __import__(pkg)
            t.add_row(pkg, "OK", "")
        except ImportError:
            t.add_row(pkg, "[red]MISSING[/]", f"pip install {pkg}")
            ok = False

    edge_ok = Path(EDGE_PATH).exists()
    t.add_row("Edge browser", "OK" if edge_ok else "[red]MISSING[/]", EDGE_PATH)
    ok = ok and edge_ok

    free = _sh.disk_usage(out_dir.anchor).free / 1e9
    t.add_row("Disk free", "OK" if free > 20 else "[yellow]LOW[/]", f"{free:.1f} GB")
    t.add_row("Sections",  "OK", ", ".join(sections))
    CONSOLE.print(t)
    log.info("Env: ok=%s free=%.1fGB sections=%s", ok, free, sections)
    return ok


# ── Per-section download orchestrator ─────────────────────────────────────────

async def download_section(section: str, args, out_dir: Path,
                           con: sqlite3.Connection, done: set,
                           csv_wr, log, stats: dict) -> None:
    sec_url = f"{BASE_URL}/" + quote(section)
    CONSOLE.rule(f"[bold magenta]Identification + Download — {section}")
    log.info("Section start: %s", section)

    queue: asyncio.Queue = asyncio.Queue(maxsize=200)

    opt = ChromiumOptions()
    opt.binary_location = EDGE_PATH
    opt.add_argument("--disable-blink-features=AutomationControlled")
    opt.add_argument("--no-sandbox")
    opt.add_argument("--window-size=1280,900")
    opt.add_argument("--lang=en-US,en")
    opt.add_argument("--disable-dev-shm-usage")

    with Progress(
        TextColumn("[>] {task.description}"),
        BarColumn(bar_width=26),
        DownloadColumn(binary_units=False),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        console=CONSOLE, expand=True,
    ) as progress:
        overall = progress.add_task(section, total=None)
        conn = aiohttp.TCPConnector(
            limit=args.concurrency + 2,
            limit_per_host=args.concurrency + 2,
        )
        async with aiohttp.ClientSession(
            headers={"User-Agent": UA, "Referer": BASE_URL},
            connector=conn,
        ) as session:
            workers = [
                asyncio.create_task(
                    dl_worker(queue, session, out_dir, con, progress, overall, csv_wr, log, stats)
                )
                for _ in range(args.concurrency)
            ]

            async with Edge(options=opt) as browser:
                page = await browser.start()
                CONSOLE.print(f"  Navigating → [cyan]{sec_url}[/]")

                # Retry CF challenge up to 4 times (early rate-limiting can delay it).
                cf_ok = False
                for att in range(1, 5):
                    try:
                        await asyncio.wait_for(page.go_to(sec_url, timeout=45), timeout=50)
                    except Exception as ex:
                        log.warning("nav attempt %d: %s", att, ex)
                    if await _wait_cf(page, args.cf_timeout, log):
                        cf_ok = True
                        break
                    log.warning("[CF] not cleared (attempt %d/4)", att)
                    CONSOLE.print(f"  [yellow]CF not cleared (attempt {att}/4) — retrying...[/]")
                    await asyncio.sleep(15 * att)

                if not cf_ok:
                    CONSOLE.print("[red]  Cloudflare did not clear after 4 attempts — skipping section[/]")
                    log.warning("Section %s skipped: CF never cleared", section)
                elif "/" in section:
                    # Sub-collection (e.g. "Samples/Argus Collection") — root may show files directly.
                    kids, nfiles = await _wait_ready_any(page, section, timeout_s=40, log=log)
                    CONSOLE.print(f"  [green]Ready[/] — {kids} folders, {nfiles} files. Crawling...\n")
                    await crawl(page, section, f"{section}/", 0, args.max_depth,
                                queue, con, done, log, stats)
                    log.info("Section %s crawl done. found=%d", section, stats["found"])
                    CONSOLE.print(f"\n  {section}: {stats['found']} found. Draining queue...\n")
                else:
                    n = await _wait_paths(page, minpaths=30, timeout_s=30, log=log)
                    CONSOLE.print(f"  [green]Ready[/] — {n} paths. Crawling...\n")
                    top = await _children(page, f"{section}/")
                    if args.limit:
                        top = top[: args.limit]
                    CONSOLE.print(f"  Top-level folders: {len(top)}\n")
                    log.info("Top-level: %d", len(top))

                    for idx, tf in enumerate(top, 1):
                        if not await _click(page, tf):
                            await _goto_root(page, sec_url, section, args.cf_timeout, log)
                            if not await _click(page, tf):
                                log.warning("skip top %s", tf)
                                continue
                        await _wait_content(page, tf)
                        await _delay()
                        await crawl(page, section, tf, 1, args.max_depth,
                                    queue, con, done, log, stats)
                        await _goto_root(page, sec_url, section, args.cf_timeout, log)
                        CONSOLE.log(
                            f"  [{section} {idx}/{len(top)}] {tf}  "
                            f"(found {stats['found']}, done {stats['done']}, fail {stats['fail']})"
                        )
                    log.info("Section %s crawl done. found=%d", section, stats["found"])
                    CONSOLE.print(f"\n  {section}: {stats['found']} found. Draining queue...\n")

            for _ in range(args.concurrency):
                await queue.put(SENTINEL)
            await asyncio.gather(*workers)


# ── Main orchestrator ─────────────────────────────────────────────────────────

async def run(args) -> None:
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    log = setup_log(out_dir / "vxdl.log")
    con  = open_db(out_dir / DB_NAME)
    done = db_done_set(con)
    stats: dict = {"found": 0, "done": 0, "fail": 0, "mib": 0.0}

    CONSOLE.rule("[bold magenta]VX-Underground Malware Collection Pipeline")
    CONSOLE.print(
        f"  Output: {out_dir}   Sections: {', '.join(args.sections)}\n"
        f"  Concurrency: {args.concurrency}   Depth: {args.max_depth}   "
        f"Resume: {len(done)} already done\n"
    )

    # Stage 0 — environment feasibility
    CONSOLE.rule("[bold]Stage 0: Feasibility")
    if not stage_env(out_dir, args.sections, log) and not args.force:
        CONSOLE.print("[red]Environment check failed. Use --force to override.[/]")
        return

    # Stage 1 — identification + download
    if args.stage in (None, "download", "identify"):
        csv_path = out_dir / "manifest.csv"
        csv_new  = not csv_path.exists()
        csv_fh   = csv_path.open("a", newline="", encoding="utf-8")
        csv_wr   = csv.DictWriter(
            csv_fh,
            fieldnames=["ts", "section", "folder", "filename", "size_mb", "sha256", "url"],
        )
        if csv_new:
            csv_wr.writeheader()
        for section in args.sections:
            await download_section(section, args, out_dir, con, done, csv_wr, log, stats)
        csv_fh.close()

    # Stage 2 — sanitation
    CONSOLE.rule("[bold]Stage 2: Sanitation")
    if args.stage in (None, "sanitize"):
        stage_sanitize(con, log)

    # Stage 3 — classification
    CONSOLE.rule("[bold]Stage 3: Classification")
    if args.stage in (None, "classify"):
        stage_classify(con, log)

    # Stage 4 — report
    CONSOLE.rule("[bold]Stage 4: Report")
    if args.stage in (None, "report"):
        stage_report(con, out_dir, log)

    CONSOLE.rule("[bold magenta]Pipeline Complete")
    CONSOLE.print(
        f"  Downloaded this run: {stats['done']}  ({stats['mib']/1024:.2f} GB)\n"
        f"  Failed: {stats['fail']}   Output: {out_dir}"
    )
    log.info("PIPELINE DONE done=%d fail=%d found=%d", stats["done"], stats["fail"], stats["found"])


def main() -> None:
    p = argparse.ArgumentParser(
        description="VX-Underground bulk downloader — Cloudflare bypass via real Edge session",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Sections (pass one or more):
  Builders                        malware builder kits
  Papers                          research papers / write-ups
  Samples                         all samples (very large)
  "Samples/Argus Collection"      Argus sub-collection only
  "Samples/Virusshare Collection" VirusShare sub-collection only
  "Samples/Bazaar Collection"     Bazaar sub-collection only

Examples:
  python vxdl.py                                           # Builders only
  python vxdl.py --sections Papers Builders --concurrency 6
  python vxdl.py --sections "Samples/Argus Collection" --max-depth 6
  python vxdl.py --limit 5                                # test: 5 top folders
  python vxdl.py --stage report                           # report only (no download)
        """,
    )
    p.add_argument(
        "--sections", nargs="+", default=["Builders"],
        help="Sections to collect (default: Builders)",
    )
    p.add_argument("--out",         default=str(OUTPUT_DIR),
                   help="Output directory (env VXUG_OUT)")
    p.add_argument("--concurrency", type=int, default=int(os.environ.get("VXUG_CONCURRENCY", 4)),
                   help="Parallel download workers (default: 4)")
    p.add_argument("--cf-timeout",  type=int, default=90,
                   help="Seconds to wait for Cloudflare to clear (default: 90)")
    p.add_argument("--max-depth",   type=int, default=5,
                   help="Maximum folder depth to recurse (default: 5)")
    p.add_argument("--limit",       type=int, default=0,
                   help="Only first N top-level folders per section; 0 = all")
    p.add_argument("--stage",
                   choices=["download", "identify", "sanitize", "classify", "report"],
                   default=None,
                   help="Run a single stage only (default: full pipeline)")
    p.add_argument("--force", action="store_true",
                   help="Continue even if environment check fails")
    asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    main()
