#!/usr/bin/env python3
"""
Watchdog — keeps vxdl.py alive indefinitely.

Detects:
  - Process crash (non-zero exit)   → immediate restart
  - Process hang (no disk progress AND no log growth for HANG_TIMEOUT seconds)
    → SIGTERM / taskkill + restart
  - Clean success (exit 0 after full pipeline) → stop

Usage:
    python watchdog.py                                     # Builders only
    python watchdog.py --sections Papers Builders --concurrency 4
    python watchdog.py --hours 24 --hang 600
"""
from __future__ import annotations

import argparse, datetime, os, signal, sqlite3, subprocess, sys, time
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

HERE     = Path(__file__).parent
SCRIPT   = HERE / "vxdl.py"
OUT_DIR  = Path(os.environ.get("VXUG_OUT", HERE / "output"))
WLOG     = OUT_DIR / "watchdog.log"
PIPLOG   = OUT_DIR / "vxdl.log"


# ── Logging ───────────────────────────────────────────────────────────────────

def wlog(msg: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    line = f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    with WLOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


# ── Progress signals ──────────────────────────────────────────────────────────

def _disk_bytes(sections: list[str]) -> int:
    """Bytes written across all section output directories."""
    total = 0
    for sec in sections:
        d = OUT_DIR / Path(sec.split("/")[0])
        if d.is_dir():
            for f in d.rglob("*"):
                try:
                    total += f.stat().st_size
                except OSError:
                    pass
    return total

def _log_size() -> int:
    try:
        return PIPLOG.stat().st_size
    except OSError:
        return 0

def _db_counts() -> tuple[int, int, int]:
    db = OUT_DIR / "vxdl.db"
    if not db.exists():
        return 0, 0, 0
    try:
        con   = sqlite3.connect(str(db), timeout=3)
        total = con.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        done  = con.execute("SELECT COUNT(*) FROM files WHERE status='done'").fetchone()[0]
        fail  = con.execute("SELECT COUNT(*) FROM files WHERE status='failed'").fetchone()[0]
        con.close()
        return total, done, fail
    except Exception:
        return 0, 0, 0


# ── Process control ───────────────────────────────────────────────────────────

def _kill(proc: subprocess.Popen) -> None:
    """Best-effort termination that works on both Windows and POSIX."""
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                capture_output=True,
            )
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except Exception:
        pass
    try:
        proc.wait(timeout=10)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(args) -> None:
    deadline = time.time() + args.hours * 3600
    attempt  = 0

    cmd = [
        sys.executable, str(SCRIPT),
        "--sections", *args.sections,
        "--out",          str(OUT_DIR),
        "--concurrency",  str(args.concurrency),
        "--max-depth",    str(args.max_depth),
        "--cf-timeout",   str(args.cf_timeout),
        "--force",
    ]
    wlog(f"Watchdog start — budget={args.hours}h hang={args.hang}s")
    wlog(f"  cmd: {' '.join(cmd)}")

    while time.time() < deadline:
        attempt += 1
        wlog(f"Launch attempt #{attempt}")
        OUT_DIR.mkdir(parents=True, exist_ok=True)

        stdout_log = OUT_DIR / "pipeline_stdout.log"
        with stdout_log.open("a", encoding="utf-8") as fh:
            proc = subprocess.Popen(
                cmd,
                cwd=str(HERE),
                stdout=fh,
                stderr=fh,
                stdin=subprocess.DEVNULL,
            )

        wlog(f"PID={proc.pid}")

        prev_disk = _disk_bytes(args.sections)
        prev_log  = _log_size()
        last_prog = time.time()

        while True:
            time.sleep(30)

            rc = proc.poll()
            if rc is not None:
                if rc == 0:
                    total, done, fail = _db_counts()
                    wlog(f"Pipeline exited 0 (success) — total={total} done={done} fail={fail}")
                    return
                wlog(f"Pipeline exited rc={rc} — restarting in 10s")
                break

            # Hang detection: measure both disk and log growth.
            cur_disk = _disk_bytes(args.sections)
            cur_log  = _log_size()
            if cur_disk > prev_disk or cur_log > prev_log:
                prev_disk = cur_disk
                prev_log  = cur_log
                last_prog = time.time()
            elif time.time() - last_prog > args.hang:
                total, done, fail = _db_counts()
                wlog(
                    f"HANG detected — no progress for {args.hang}s "
                    f"(total={total} done={done} fail={fail}) — killing"
                )
                _kill(proc)
                break

        time.sleep(10)

    wlog("Budget exhausted → exit")
    total, done, fail = _db_counts()
    wlog(f"Final: total={total} done={done} fail={fail}")


def main() -> None:
    p = argparse.ArgumentParser(description="Watchdog for vxdl.py — auto-restart on crash/hang")
    p.add_argument("--sections",    nargs="+", default=["Builders"],
                   help="Sections passed through to vxdl.py")
    p.add_argument("--out",         default=str(OUT_DIR),
                   help="Output directory")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--max-depth",   type=int, default=5)
    p.add_argument("--cf-timeout",  type=int, default=90)
    p.add_argument("--hours",       type=float, default=48,
                   help="Maximum total run budget in hours (default: 48)")
    p.add_argument("--hang",        type=int, default=360,
                   help="Seconds without progress before declaring a hang (default: 360)")
    run(p.parse_args())


if __name__ == "__main__":
    main()
