#!/usr/bin/env python3
"""
Analyze maven-profiler JSON reports and output Markdown summary.

Usage:
  # Single report analysis
  ./dev/analyze-build-profile.py .profiler/profiler-report-*.json

  # Compare two reports (e.g., clean vs incremental)
  ./dev/analyze-build-profile.py --compare clean.json incremental.json

  # Auto-pick latest report(s)
  ./dev/analyze-build-profile.py --latest
  ./dev/analyze-build-profile.py --latest --compare
"""

import argparse
import glob
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path


# =============================================================================
# Parsing
# =============================================================================

def parse_time_ms(time_str: str) -> int:
    """Parse '49612 ms' -> 49612."""
    m = re.match(r"(\d+)\s*ms", time_str.strip())
    return int(m.group(1)) if m else 0


def parse_mojo_name(mojo_str: str):
    """
    Parse 'groupId:artifactId:version:goal {execution: id}'
    Returns (short_plugin, goal, execution_id).
    """
    exec_id = ""
    m = re.search(r"\{execution:\s*([^}]+)\}", mojo_str)
    if m:
        exec_id = m.group(1).strip()

    parts_str = re.sub(r"\s*\{.*\}", "", mojo_str).strip()
    parts = parts_str.split(":")
    if len(parts) >= 4:
        artifact = parts[1]
        goal = parts[3]
    elif len(parts) >= 2:
        artifact = parts[0]
        goal = parts[-1]
    else:
        artifact = parts_str
        goal = ""

    return artifact, goal, exec_id


def load_report(path: str) -> dict:
    """Load and normalize a profiler JSON report."""
    with open(path) as f:
        data = json.load(f)

    report = {
        "file": os.path.basename(path),
        "name": data.get("name", ""),
        "date": data.get("date", ""),
        "goals": data.get("goals", ""),
        "total_ms": parse_time_ms(data.get("time", "0 ms")),
        "modules": [],
    }

    for proj in data.get("projects", []):
        module = {
            "name": proj["project"],
            "time_ms": parse_time_ms(proj.get("time", "0 ms")),
            "mojos": [],
        }
        for mojo in proj.get("mojos", []):
            plugin, goal, exec_id = parse_mojo_name(mojo["mojo"])
            module["mojos"].append({
                "plugin": plugin,
                "goal": goal,
                "exec_id": exec_id,
                "time_ms": parse_time_ms(mojo.get("time", "0 ms")),
                "raw": mojo["mojo"],
            })
        report["modules"].append(module)

    return report


# =============================================================================
# Formatting helpers
# =============================================================================

def fmt_ms(ms: int) -> str:
    """Format milliseconds to human-readable string."""
    if ms >= 60000:
        return f"{ms / 60000:.1f}m"
    elif ms >= 1000:
        return f"{ms / 1000:.1f}s"
    else:
        return f"{ms}ms"


def bar(ratio: float, width: int = 20) -> str:
    """Generate a text bar chart."""
    filled = int(ratio * width)
    return "█" * filled + "░" * (width - filled)


def pct(part: int, total: int) -> str:
    if total == 0:
        return "0.0%"
    return f"{part / total * 100:.1f}%"


# =============================================================================
# Analysis: Single Report
# =============================================================================

def section_header(report: dict) -> str:
    lines = [
        "# Maven Build Profile Analysis",
        "",
        "| Item | Value |",
        "|------|-------|",
        f"| **Project** | {report['name']} |",
        f"| **Date** | {report['date']} |",
        f"| **Goals** | `{report['goals']}` |",
        f"| **Total Time** | **{fmt_ms(report['total_ms'])}** ({report['total_ms']}ms) |",
        f"| **Modules** | {len(report['modules'])} |",
        "",
    ]
    return "\n".join(lines)


def section_modules(report: dict) -> str:
    """Module time ranking."""
    total = report["total_ms"]
    modules = sorted(report["modules"], key=lambda m: m["time_ms"], reverse=True)
    max_time = modules[0]["time_ms"] if modules else 1

    lines = [
        "## 1. Module Time Ranking",
        "",
        "| # | Module | Time | % | Distribution |",
        "|---|--------|------|---|-------------|",
    ]
    for i, m in enumerate(modules, 1):
        ratio = m["time_ms"] / max_time if max_time > 0 else 0
        lines.append(
            f"| {i} | {m['name']} | {fmt_ms(m['time_ms'])} | {pct(m['time_ms'], total)} | `{bar(ratio)}` |"
        )

    sum_modules = sum(m["time_ms"] for m in modules)
    overhead = total - sum_modules
    if overhead > 100:
        lines.append(f"| | *Maven overhead* | {fmt_ms(overhead)} | {pct(overhead, total)} | |")

    lines.append("")
    return "\n".join(lines)


def section_top_mojos(report: dict, top_n: int = 15) -> str:
    """Top N slowest mojo executions across all modules."""
    all_mojos = []
    for m in report["modules"]:
        for mojo in m["mojos"]:
            if mojo["time_ms"] > 0:
                all_mojos.append({
                    "module": m["name"],
                    "plugin": mojo["plugin"],
                    "goal": mojo["goal"],
                    "exec_id": mojo["exec_id"],
                    "time_ms": mojo["time_ms"],
                })

    all_mojos.sort(key=lambda x: x["time_ms"], reverse=True)
    total = report["total_ms"]

    lines = [
        f"## 2. Top {top_n} Slowest Mojo Executions",
        "",
        "| # | Module | Plugin:Goal | Time | % |",
        "|---|--------|-------------|------|---|",
    ]
    for i, mojo in enumerate(all_mojos[:top_n], 1):
        lines.append(
            f"| {i} | {mojo['module']} | `{mojo['plugin']}:{mojo['goal']}` | {fmt_ms(mojo['time_ms'])} | {pct(mojo['time_ms'], total)} |"
        )
    lines.append("")
    return "\n".join(lines)


def section_goal_aggregate(report: dict) -> str:
    """Aggregate time by goal type across all modules."""
    goal_times = defaultdict(int)
    for m in report["modules"]:
        for mojo in m["mojos"]:
            key = f"{mojo['plugin']}:{mojo['goal']}"
            goal_times[key] += mojo["time_ms"]

    sorted_goals = sorted(goal_times.items(), key=lambda x: x[1], reverse=True)
    total = report["total_ms"]

    lines = [
        "## 3. Time by Goal Type (Aggregated)",
        "",
        "| Goal | Total Time | % |",
        "|------|-----------|---|",
    ]
    for goal, ms in sorted_goals:
        if ms > 0:
            lines.append(f"| `{goal}` | {fmt_ms(ms)} | {pct(ms, total)} |")
    lines.append("")
    return "\n".join(lines)


def section_category_breakdown(report: dict) -> str:
    """Categorize time into compile/testCompile/other."""
    categories = {
        "scala:compile": 0,
        "scala:testCompile": 0,
        "java:compile": 0,
        "java:testCompile": 0,
        "other": 0,
    }

    for m in report["modules"]:
        for mojo in m["mojos"]:
            if mojo["plugin"] == "scala-maven-plugin" and mojo["goal"] == "compile":
                categories["scala:compile"] += mojo["time_ms"]
            elif mojo["plugin"] == "scala-maven-plugin" and mojo["goal"] == "testCompile":
                categories["scala:testCompile"] += mojo["time_ms"]
            elif mojo["plugin"] == "maven-compiler-plugin" and mojo["goal"] == "compile":
                categories["java:compile"] += mojo["time_ms"]
            elif mojo["plugin"] == "maven-compiler-plugin" and mojo["goal"] == "testCompile":
                categories["java:testCompile"] += mojo["time_ms"]
            else:
                categories["other"] += mojo["time_ms"]

    total = report["total_ms"]
    sum_cat = sum(categories.values())
    overhead = total - sum_cat

    lines = [
        "## 4. Time by Category",
        "",
        "| Category | Time | % | Bar |",
        "|----------|------|---|-----|",
    ]
    max_cat = max(categories.values()) if categories else 1
    for cat, ms in sorted(categories.items(), key=lambda x: x[1], reverse=True):
        ratio = ms / max_cat if max_cat > 0 else 0
        lines.append(f"| **{cat}** | {fmt_ms(ms)} | {pct(ms, total)} | `{bar(ratio, 15)}` |")

    if overhead > 100:
        lines.append(f"| *maven overhead* | {fmt_ms(overhead)} | {pct(overhead, total)} | |")

    scala_total = categories["scala:compile"] + categories["scala:testCompile"]
    lines.extend([
        "",
        f"> **Scala compilation**: {fmt_ms(scala_total)} ({pct(scala_total, total)} of total)",
    ])
    lines.append("")
    return "\n".join(lines)


def section_per_module_breakdown(report: dict) -> str:
    """Per-module compile vs testCompile breakdown."""
    modules = sorted(report["modules"], key=lambda m: m["time_ms"], reverse=True)

    lines = [
        "## 5. Per-Module Compile Breakdown",
        "",
        "| Module | compile | testCompile | other | total |",
        "|--------|---------|-------------|-------|-------|",
    ]
    for m in modules:
        if m["time_ms"] < 100:
            continue
        compile_ms = sum(
            mj["time_ms"] for mj in m["mojos"]
            if mj["plugin"] == "scala-maven-plugin" and mj["goal"] == "compile"
        )
        test_compile_ms = sum(
            mj["time_ms"] for mj in m["mojos"]
            if mj["plugin"] == "scala-maven-plugin" and mj["goal"] == "testCompile"
        )
        other_ms = m["time_ms"] - compile_ms - test_compile_ms
        lines.append(
            f"| {m['name']} | {fmt_ms(compile_ms)} | {fmt_ms(test_compile_ms)} | {fmt_ms(other_ms)} | {fmt_ms(m['time_ms'])} |"
        )
    lines.append("")
    return "\n".join(lines)


def analyze_single(report: dict) -> str:
    """Full single-report analysis."""
    return "\n".join([
        section_header(report),
        section_modules(report),
        section_top_mojos(report),
        section_goal_aggregate(report),
        section_category_breakdown(report),
        section_per_module_breakdown(report),
    ])


# =============================================================================
# Analysis: Compare Two Reports
# =============================================================================

def _delta(a_ms: int, b_ms: int) -> str:
    """Format delta with sign."""
    diff = b_ms - a_ms
    if diff == 0:
        return "—"
    sign = "+" if diff > 0 else ""
    return f"{sign}{fmt_ms(diff)}"


def _speedup(a_ms: int, b_ms: int) -> str:
    """Calculate speedup ratio."""
    if a_ms == 0:
        return "—"
    if b_ms == 0:
        return "∞"
    ratio = a_ms / b_ms
    if ratio > 1:
        return f"**{ratio:.1f}x** faster"
    elif ratio < 1:
        return f"{1/ratio:.1f}x slower"
    else:
        return "—"


def analyze_compare(report_a: dict, report_b: dict) -> str:
    """Compare two profiler reports side by side."""
    lines = [
        "# Maven Build Profile Comparison",
        "",
        "| | A (Baseline) | B (Current) | Δ |",
        "|---|---|---|---|",
        f"| **File** | `{report_a['file']}` | `{report_b['file']}` | |",
        f"| **Date** | {report_a['date']} | {report_b['date']} | |",
        f"| **Total** | {fmt_ms(report_a['total_ms'])} | {fmt_ms(report_b['total_ms'])} | {_delta(report_a['total_ms'], report_b['total_ms'])} |",
        "",
    ]

    # Module comparison
    lines.extend([
        "## Module Comparison",
        "",
        "| Module | A | B | Δ | Speedup |",
        "|--------|---|---|---|---------|",
    ])

    mods_a = {m["name"]: m for m in report_a["modules"]}
    mods_b = {m["name"]: m for m in report_b["modules"]}
    all_names = list(dict.fromkeys(
        [m["name"] for m in report_a["modules"]] +
        [m["name"] for m in report_b["modules"]]
    ))

    for name in all_names:
        a_ms = mods_a[name]["time_ms"] if name in mods_a else 0
        b_ms = mods_b[name]["time_ms"] if name in mods_b else 0
        if a_ms == 0 and b_ms == 0:
            continue
        lines.append(
            f"| {name} | {fmt_ms(a_ms)} | {fmt_ms(b_ms)} | {_delta(a_ms, b_ms)} | {_speedup(a_ms, b_ms)} |"
        )

    lines.append("")

    # Category comparison
    lines.extend([
        "## Category Comparison",
        "",
        "| Category | A | B | Δ |",
        "|----------|---|---|---|",
    ])

    for cat_name, cat_filter in [
        ("scala:compile", lambda mj: mj["plugin"] == "scala-maven-plugin" and mj["goal"] == "compile"),
        ("scala:testCompile", lambda mj: mj["plugin"] == "scala-maven-plugin" and mj["goal"] == "testCompile"),
        ("other", lambda mj: not (mj["plugin"] == "scala-maven-plugin" and mj["goal"] in ("compile", "testCompile"))),
    ]:
        a_ms = sum(mj["time_ms"] for m in report_a["modules"] for mj in m["mojos"] if cat_filter(mj))
        b_ms = sum(mj["time_ms"] for m in report_b["modules"] for mj in m["mojos"] if cat_filter(mj))
        lines.append(f"| **{cat_name}** | {fmt_ms(a_ms)} | {fmt_ms(b_ms)} | {_delta(a_ms, b_ms)} |")

    lines.append("")

    # Per-module mojo diff (only significant changes)
    lines.extend([
        "## Significant Changes (|Δ| > 500ms)",
        "",
        "| Module | Goal | A | B | Δ |",
        "|--------|------|---|---|---|",
    ])

    for name in all_names:
        mojos_a = {(mj["plugin"], mj["goal"], mj["exec_id"]): mj["time_ms"]
                   for mj in mods_a.get(name, {}).get("mojos", [])} if name in mods_a else {}
        mojos_b = {(mj["plugin"], mj["goal"], mj["exec_id"]): mj["time_ms"]
                   for mj in mods_b.get(name, {}).get("mojos", [])} if name in mods_b else {}
        all_keys = set(mojos_a.keys()) | set(mojos_b.keys())
        for key in sorted(all_keys, key=lambda k: abs(mojos_a.get(k, 0) - mojos_b.get(k, 0)), reverse=True):
            a_ms = mojos_a.get(key, 0)
            b_ms = mojos_b.get(key, 0)
            if abs(b_ms - a_ms) > 500:
                lines.append(
                    f"| {name} | `{key[0]}:{key[1]}` | {fmt_ms(a_ms)} | {fmt_ms(b_ms)} | {_delta(a_ms, b_ms)} |"
                )

    lines.append("")
    return "\n".join(lines)


# =============================================================================
# File Discovery
# =============================================================================

def find_latest_reports(base_dir: str, count: int = 1) -> list:
    """Find the latest N profiler JSON reports."""
    pattern = os.path.join(base_dir, ".profiler", "profiler-report-*.json")
    files = sorted(glob.glob(pattern), reverse=True)
    if not files:
        print(f"No JSON reports found in {base_dir}/.profiler/", file=sys.stderr)
        print("Run build with: -Dprofile -DprofileFormat=JSON", file=sys.stderr)
        sys.exit(1)
    return files[:count]


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Analyze maven-profiler JSON reports and output Markdown.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s .profiler/report.json                  # Analyze single report
  %(prog)s --compare clean.json incremental.json  # Compare two reports
  %(prog)s --latest                               # Analyze latest report
  %(prog)s --latest --compare                     # Compare latest two reports
  %(prog)s report.json -o report.md               # Save to file
        """,
    )
    parser.add_argument("files", nargs="*", help="JSON report file(s)")
    parser.add_argument("--compare", action="store_true",
                        help="Compare two reports (A=baseline, B=current)")
    parser.add_argument("--latest", action="store_true",
                        help="Auto-pick latest report(s) from .profiler/")
    parser.add_argument("--dir", default=".",
                        help="Project root directory (default: .)")
    parser.add_argument("-o", "--output",
                        help="Output file (default: stdout)")
    parser.add_argument("-n", "--top", type=int, default=15,
                        help="Top N mojos to show (default: 15)")

    args = parser.parse_args()

    # Resolve input files
    if args.latest:
        count = 2 if args.compare else 1
        files = find_latest_reports(args.dir, count)
        if args.compare and len(files) < 2:
            print("Need at least 2 JSON reports for comparison.",
                  file=sys.stderr)
            sys.exit(1)
    elif args.files:
        files = args.files
    else:
        files = find_latest_reports(args.dir, 2 if args.compare else 1)

    # Validate files exist
    for f in files:
        if not os.path.isfile(f):
            print(f"File not found: {f}", file=sys.stderr)
            sys.exit(1)

    # Run analysis
    if args.compare:
        if len(files) < 2:
            print("Need exactly 2 files for comparison.", file=sys.stderr)
            sys.exit(1)
        report_a = load_report(files[1])  # older = baseline
        report_b = load_report(files[0])  # newer = current
        output = analyze_compare(report_a, report_b)
    else:
        report = load_report(files[0])
        output = analyze_single(report)

    # Output
    if args.output:
        Path(args.output).write_text(output)
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
