#!/usr/bin/env python3
"""
report.py — standalone report generator for the VX-Underground collection.

Read-only: safe to run while the downloader is still active.
Writes report.md (Markdown) and report.txt (plain-text mirror) to the output directory.

Usage:
    python report.py                          # auto-locate DB from VXUG_OUT or ./output
    python report.py --db /path/to/vxdl.db --out /path/to/output
"""
from __future__ import annotations

import argparse, datetime, os, shutil, sqlite3, sys
from collections import Counter
from pathlib import Path

# Import taxonomy from the main pipeline without running it.
from vxdl import PLATFORM, CLASS_RULES, IMPACT, _classify, _check_magic  # noqa: F401


def run(db_path: Path, out_dir: Path) -> None:
    con = sqlite3.connect(str(db_path), timeout=10)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT section,folder,filename,size_bytes,sha256,local_path,status "
        "FROM files WHERE status='done'"
    ).fetchall()
    found_total = con.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    failed      = con.execute("SELECT COUNT(*) FROM files WHERE status='failed'").fetchone()[0]
    con.close()

    n           = len(rows)
    total_bytes = sum(r["size_bytes"] for r in rows)

    by_section      : Counter = Counter()
    sec_bytes       : Counter = Counter()
    by_platform     : Counter = Counter()
    by_impact       : Counter = Counter()
    by_class        : Counter = Counter()
    class_detail    : Counter = Counter()
    folder_counts   : Counter = Counter()
    ext_counts      : Counter = Counter()
    largest: list[tuple] = []

    for r in rows:
        sec, folder, fname = r["section"], r["folder"], r["filename"]
        plat, imp, cls = _classify(sec, folder, fname)
        by_section[sec]             += 1
        sec_bytes[sec]              += r["size_bytes"]
        by_platform[plat]           += 1
        by_impact[imp]              += 1
        by_class[cls]               += 1
        class_detail[(cls,imp,plat)] += 1
        folder_counts[folder]        += 1
        ext_counts[Path(fname).suffix.lower() or "(none)"] += 1
        largest.append((r["size_bytes"], sec, fname))

    largest.sort(reverse=True)

    free = shutil.disk_usage(str(out_dir.anchor)).free
    now  = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    def gb(b: int) -> str: return f"{b / 1_073_741_824:.2f}"
    def mb(b: int) -> str: return f"{b / 1_048_576:.1f}"

    L: list[str] = []
    w = L.append

    w("# VX-Underground Malware Collection — Report")
    w(f"\n_Generated: {now}_\n")

    w("## 1. Collection Summary\n")
    w(f"| Metric | Value |")
    w(f"|---|---|")
    w(f"| Files downloaded | {n:,} |")
    w(f"| Total catalogued (found) | {found_total:,} |")
    w(f"| Failed | {failed} |")
    w(f"| Total size | {gb(total_bytes)} GB |")
    w(f"| Disk free | {gb(free)} GB |")
    w(f"| Output directory | `{out_dir}` |")
    w(f"| Database | `{db_path}` |")
    w("")

    w("## 2. By Section\n")
    w("| Section | Files | Size (GB) |")
    w("|---|---:|---:|")
    for sec, cnt in by_section.most_common():
        w(f"| {sec} | {cnt:,} | {gb(sec_bytes[sec])} |")
    w("")

    w("## 3. By Impact (severity)\n")
    w("| Impact | Files |")
    w("|---|---:|")
    for imp in ["Critical", "High", "Medium", "Low", "Info", "Unknown"]:
        if by_impact.get(imp):
            w(f"| {imp} | {by_impact[imp]:,} |")
    w("")

    w("## 4. By Malware Class\n")
    w("| Class | Files |")
    w("|---|---:|")
    for cls, cnt in by_class.most_common():
        w(f"| {cls} | {cnt:,} |")
    w("")

    w("## 5. By Platform\n")
    w("| Platform | Files |")
    w("|---|---:|")
    for plat, cnt in by_platform.most_common():
        w(f"| {plat} | {cnt:,} |")
    w("")

    w("## 6. Class × Impact × Platform (top 30)\n")
    w("| Class | Impact | Platform | Files |")
    w("|---|---|---|---:|")
    for (cls, imp, plat), cnt in class_detail.most_common(30):
        w(f"| {cls} | {imp} | {plat} | {cnt:,} |")
    w("")

    w("## 7. File Extensions (top 20)\n")
    w("| Extension | Files |")
    w("|---|---:|")
    for ext, cnt in ext_counts.most_common(20):
        w(f"| {ext} | {cnt:,} |")
    w("")

    w("## 8. Top 25 Folders by File Count\n")
    w("| Folder | Files |")
    w("|---|---:|")
    for folder, cnt in folder_counts.most_common(25):
        w(f"| {folder} | {cnt} |")
    w("")

    w("## 9. Largest 20 Files\n")
    w("| Size (MB) | Section | File |")
    w("|---:|---|---|")
    for size, sec, fname in largest[:20]:
        w(f"| {mb(size)} | {sec} | {fname} |")
    w("")

    w("---")
    w("_Pipeline stages: Feasibility → Identification (crawl) → Download → Sanitation → Classification_")

    md = "\n".join(L)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "report.md").write_text(md,  encoding="utf-8")
    (out_dir / "report.txt").write_text(md, encoding="utf-8")

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    print(md)
    print(f"\nSaved → {out_dir / 'report.md'}  and  {out_dir / 'report.txt'}")


def main() -> None:
    default_out = Path(os.environ.get("VXUG_OUT", Path(__file__).parent / "output"))
    p = argparse.ArgumentParser(description="Standalone report generator for vxdl.py collection")
    p.add_argument("--db",  default=str(default_out / "vxdl.db"),
                   help="Path to vxdl.db (default: <out>/vxdl.db)")
    p.add_argument("--out", default=str(default_out),
                   help="Output directory for report files (default: ./output)")
    args = p.parse_args()
    db   = Path(args.db)
    out  = Path(args.out)
    if not db.exists():
        print(f"[error] Database not found: {db}", file=sys.stderr)
        sys.exit(1)
    run(db, out)


if __name__ == "__main__":
    main()
