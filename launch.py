#!/usr/bin/env python3
"""
launch.py — start the watchdog fully detached so it survives session/terminal close.

On Windows this uses DETACHED_PROCESS flags; on POSIX it double-forks.
The watchdog in turn launches vxdl.py and auto-restarts it on crash or hang.

Usage:
    python launch.py                                      # Builders only
    python launch.py --sections Papers Builders --hours 24
    python launch.py --sections "Samples/Argus Collection" --concurrency 6
"""
from __future__ import annotations

import argparse, os, sys
from pathlib import Path

HERE    = Path(__file__).parent
SCRIPT  = HERE / "watchdog.py"
OUT_DIR = Path(os.environ.get("VXUG_OUT", HERE / "output"))


def _launch_windows(cmd: list[str], stdout_log: Path) -> int:
    import subprocess

    DETACHED_PROCESS       = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200
    CREATE_NO_WINDOW       = 0x08000000

    with stdout_log.open("a", encoding="utf-8") as fh:
        proc = subprocess.Popen(
            cmd,
            cwd=str(HERE),
            stdout=fh,
            stderr=fh,
            stdin=subprocess.DEVNULL,
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
            close_fds=True,
        )
    return proc.pid


def _launch_posix(cmd: list[str], stdout_log: Path) -> int:
    """Double-fork so the child is reparented to init and fully detached."""
    rpid, wpid = os.pipe()
    if os.fork() != 0:
        os.close(wpid)
        with os.fdopen(rpid) as f:
            pid = int(f.read())
        return pid
    # first child
    os.setsid()
    os.close(rpid)
    if os.fork() != 0:
        os._exit(0)
    # second child — fully detached
    pid = os.getpid()
    with os.fdopen(wpid, "w") as f:
        f.write(str(pid))
    with stdout_log.open("a") as fh:
        os.dup2(fh.fileno(), sys.stdout.fileno())
        os.dup2(fh.fileno(), sys.stderr.fileno())
    os.execv(sys.executable, cmd)
    os._exit(1)  # unreachable


def run(args) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stdout_log = OUT_DIR / "watchdog_stdout.log"
    pid_file   = OUT_DIR / "watchdog.pid"

    cmd = [
        sys.executable, str(SCRIPT),
        "--sections", *args.sections,
        "--out",          args.out,
        "--concurrency",  str(args.concurrency),
        "--max-depth",    str(args.max_depth),
        "--cf-timeout",   str(args.cf_timeout),
        "--hours",        str(args.hours),
        "--hang",         str(args.hang),
    ]

    if sys.platform == "win32":
        pid = _launch_windows(cmd, stdout_log)
    else:
        pid = _launch_posix(cmd, stdout_log)

    pid_file.write_text(str(pid))
    print(f"Watchdog launched  PID={pid}")
    print(f"  stdout  → {stdout_log}")
    print(f"  pid     → {pid_file}")
    print(f"  output  → {args.out}")
    print(f"  sections: {', '.join(args.sections)}")
    print("Process is fully detached — survives terminal/session close.")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Launch the vxdl watchdog detached (survives session close)"
    )
    p.add_argument("--sections",    nargs="+", default=["Builders"])
    p.add_argument("--out",         default=str(OUT_DIR))
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--max-depth",   type=int, default=5)
    p.add_argument("--cf-timeout",  type=int, default=90)
    p.add_argument("--hours",       type=float, default=48,
                   help="Max watchdog budget in hours")
    p.add_argument("--hang",        type=int, default=360,
                   help="Hang detection threshold in seconds")
    run(p.parse_args())


if __name__ == "__main__":
    main()
