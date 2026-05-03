#!/usr/bin/env python3
"""Generate runtime comparison reports from Criterion benchmark JSON output."""

from __future__ import annotations

import argparse
import html
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


COLORS = [
    "#2563eb",
    "#dc2626",
    "#059669",
    "#9333ea",
    "#d97706",
    "#0891b2",
    "#be123c",
    "#4f46e5",
]
WIDTH = 900
HEIGHT = 360
MARGIN_LEFT = 82
MARGIN_RIGHT = 28
MARGIN_TOP = 30
MARGIN_BOTTOM = 62


@dataclass(frozen=True)
class BenchKey:
    suite: str
    workload: str
    transport: str
    variant: str
    size: int


@dataclass(frozen=True)
class BenchPoint:
    series: str
    impl: str
    baseline: str
    key: BenchKey
    full_id: str
    estimate_ns: float
    lower_ns: float | None
    upper_ns: float | None
    throughput_bytes: int | None


@dataclass(frozen=True)
class Series:
    label: str
    points: dict[BenchKey, BenchPoint]


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.run_dir:
        run_dir = Path(args.run_dir)
        all_points, subtitle = load_run_points(run_dir)
    else:
        criterion_dir = Path(args.criterion_dir)
        all_points = load_points(
            criterion_dir,
            suite="criterion",
            series_label=None,
            impl_filter=None,
        )
        subtitle = f"Criterion directory: {criterion_dir}"

    series = build_series(all_points)
    output = out_dir / "index.html"
    output.write_text(
        render_report("libzmq vs zmqrs runtime comparison", subtitle, series),
        encoding="utf-8",
    )

    print(f"Loaded {len(all_points)} Criterion measurements")
    print(f"Series: {', '.join(item.label for item in series) or '(none)'}")
    print(f"Wrote {output}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate standalone HTML comparison graphs from Criterion JSON data."
    )
    parser.add_argument(
        "--run-dir",
        help="Benchmark run directory containing manifest.json from run_bench_suite.py.",
    )
    parser.add_argument(
        "--criterion-dir",
        default="target/criterion",
        help="Fallback direct Criterion directory when --run-dir is not used.",
    )
    parser.add_argument("--out-dir", default="target/criterion-comparison")
    return parser.parse_args()


def load_run_points(run_dir: Path) -> tuple[list[BenchPoint], str]:
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    points: list[BenchPoint] = []
    for entry in manifest.get("entries", []):
        if not isinstance(entry, dict):
            continue
        entry_name = str(entry.get("name", "unknown"))
        impl_filter = entry.get("impl")
        series_label = series_label_for_entry(entry)
        for bench in entry.get("benches", []):
            if not isinstance(bench, dict):
                continue
            criterion_dir = bench.get("criterion_dir")
            bench_name = bench.get("name")
            if not isinstance(criterion_dir, str) or not isinstance(bench_name, str):
                continue
            points.extend(
                load_points(
                    run_dir / criterion_dir,
                    suite=bench_name,
                    series_label=series_label,
                    impl_filter=str(impl_filter) if impl_filter else None,
                )
            )
        if not entry.get("benches"):
            print(f"Skipping {entry_name}: no bench result directories in manifest")

    created_at = manifest.get("created_at", "unknown time")
    revision = manifest.get("git_revision")
    subtitle = f"Run: {run_dir.name}, created: {created_at}"
    if revision:
        subtitle += f", git: {str(revision)[:12]}"
    return points, subtitle


def series_label_for_entry(entry: dict[str, Any]) -> str:
    name = str(entry.get("name", "unknown"))
    kind = entry.get("kind")
    impl = entry.get("impl")
    if kind == "reference" or impl == "libzmq":
        return "libzmq"
    return f"zmqrs {name}"


def load_points(
    criterion_dir: Path,
    *,
    suite: str,
    series_label: str | None,
    impl_filter: str | None,
) -> list[BenchPoint]:
    points = []
    for benchmark_path in criterion_dir.rglob("benchmark.json"):
        estimates_path = benchmark_path.with_name("estimates.json")
        if not estimates_path.exists():
            continue
        try:
            benchmark = json.loads(benchmark_path.read_text(encoding="utf-8"))
            estimates = json.loads(estimates_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Skipping {benchmark_path}: {exc}")
            continue

        point = parse_point(
            baseline=benchmark_path.parent.name,
            suite=suite,
            series_label=series_label,
            impl_filter=impl_filter,
            benchmark=benchmark,
            estimates=estimates,
        )
        if point is not None:
            points.append(point)
    return points


def parse_point(
    *,
    baseline: str,
    suite: str,
    series_label: str | None,
    impl_filter: str | None,
    benchmark: dict[str, Any],
    estimates: dict[str, Any],
) -> BenchPoint | None:
    group_id = benchmark.get("group_id")
    full_id = benchmark.get("full_id")
    value_str = benchmark.get("value_str")
    if not isinstance(group_id, str) or not isinstance(full_id, str):
        return None

    parts = group_id.split("/")
    if len(parts) < 3 or parts[0] not in {"libzmq", "zmqrs"}:
        return None
    impl = parts[0]
    if impl_filter is not None and impl != impl_filter:
        return None

    key_prefix = parse_key_prefix(parts)
    if key_prefix is None:
        return None
    workload, transport, variant = key_prefix

    try:
        size = int(value_str if value_str is not None else full_id.rsplit("/", 1)[-1])
    except (TypeError, ValueError):
        return None

    estimate = estimates.get("slope") or estimates.get("mean")
    if not isinstance(estimate, dict):
        return None
    point_estimate = estimate.get("point_estimate")
    if not isinstance(point_estimate, (int, float)) or point_estimate <= 0:
        return None

    interval = estimate.get("confidence_interval")
    lower = interval.get("lower_bound") if isinstance(interval, dict) else None
    upper = interval.get("upper_bound") if isinstance(interval, dict) else None
    throughput = benchmark.get("throughput")
    throughput_bytes = throughput.get("Bytes") if isinstance(throughput, dict) else None
    label = series_label or legacy_series_label(impl, baseline)

    return BenchPoint(
        series=label,
        impl=impl,
        baseline=baseline,
        key=BenchKey(
            suite=suite,
            workload=workload,
            transport=transport,
            variant=variant,
            size=size,
        ),
        full_id=full_id,
        estimate_ns=float(point_estimate),
        lower_ns=float(lower) if isinstance(lower, (int, float)) else None,
        upper_ns=float(upper) if isinstance(upper, (int, float)) else None,
        throughput_bytes=int(throughput_bytes) if isinstance(throughput_bytes, int) else None,
    )


def parse_key_prefix(parts: list[str]) -> tuple[str, str, str] | None:
    if len(parts) >= 4 and parts[1] == "throughput":
        return parts[2], parts[3], "/".join(parts[4:])
    if len(parts) >= 3:
        return parts[1], parts[2], "/".join(parts[3:])
    return None


def legacy_series_label(impl: str, baseline: str) -> str:
    if baseline == "new":
        return impl
    return f"{impl} {baseline}"


def build_series(points: Iterable[BenchPoint]) -> list[Series]:
    labels = []
    grouped: dict[str, dict[BenchKey, BenchPoint]] = {}
    for point in points:
        if point.series not in grouped:
            grouped[point.series] = {}
            labels.append(point.series)
        grouped[point.series][point.key] = point

    labels.sort(key=series_sort_key)
    return [Series(label=label, points=grouped[label]) for label in labels]


def series_sort_key(label: str) -> tuple[int, str]:
    if label == "libzmq":
        return (0, label)
    if label.startswith("zmqrs tokio"):
        return (1, label)
    return (2, label)


def render_report(title: str, subtitle: str, series: list[Series]) -> str:
    chart_keys = sorted(
        {
            (key.suite, key.workload, key.transport, key.variant)
            for item in series
            for key in item.points
        }
    )
    charts = []
    for suite, workload, transport, variant in chart_keys:
        keys = sorted(
            {
                key
                for item in series
                for key in item.points
                if key.suite == suite
                and key.workload == workload
                and key.transport == transport
                and key.variant == variant
            },
            key=lambda key: key.size,
        )
        chart_series = [
            Series(item.label, {key: item.points[key] for key in keys if key in item.points})
            for item in series
        ]
        if any(item.points for item in chart_series):
            charts.append(render_chart(suite, workload, transport, variant, chart_series))

    missing = render_missing(series)
    body = "\n".join(charts) if charts else '<p class="empty">No matching measurements found.</p>'
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title>
<style>
:root {{
  color-scheme: light;
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: #f8fafc;
  color: #172033;
}}
body {{
  margin: 0;
  padding: 32px;
}}
main {{
  max-width: 1160px;
  margin: 0 auto;
}}
h1 {{
  margin: 0 0 6px;
  font-size: 28px;
  line-height: 1.2;
}}
.subtitle {{
  margin: 0 0 24px;
  color: #5b6475;
}}
.chart {{
  margin: 0 0 28px;
  padding: 20px;
  border: 1px solid #dde3ee;
  border-radius: 8px;
  background: #ffffff;
  box-shadow: 0 1px 2px rgb(15 23 42 / 6%);
}}
.chart h2 {{
  margin: 0 0 12px;
  font-size: 18px;
}}
svg {{
  display: block;
  width: 100%;
  height: auto;
}}
.axis, .grid {{
  stroke: #cad3e1;
  stroke-width: 1;
}}
.grid {{
  stroke-dasharray: 3 4;
}}
.tick {{
  fill: #647084;
  font-size: 12px;
}}
.axis-label {{
  fill: #334155;
  font-size: 13px;
  font-weight: 600;
}}
.legend {{
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  margin: 10px 0 0;
  color: #334155;
  font-size: 13px;
}}
.legend span {{
  display: inline-flex;
  align-items: center;
  gap: 6px;
}}
.swatch {{
  width: 10px;
  height: 10px;
  border-radius: 50%;
}}
.table-wrap {{
  overflow-x: auto;
}}
table {{
  margin-top: 12px;
  border-collapse: collapse;
  font-size: 13px;
  min-width: 560px;
}}
th, td {{
  padding: 6px 10px;
  border-bottom: 1px solid #e5eaf2;
  text-align: right;
  white-space: nowrap;
}}
th:first-child, td:first-child {{
  text-align: left;
}}
.empty, .missing {{
  color: #647084;
}}
</style>
</head>
<body>
<main>
<h1>{escape(title)}</h1>
<p class="subtitle">{escape(subtitle)}. Lower time is better. Points use Criterion slope estimates in ns with 95% confidence intervals.</p>
{body}
{missing}
</main>
</body>
</html>
"""


def render_chart(
    suite: str, workload: str, transport: str, variant: str, series: list[Series]
) -> str:
    sizes = sorted({key.size for item in series for key in item.points})
    values = [point.estimate_ns for item in series for point in item.points.values()]
    lowers = [
        point.lower_ns
        for item in series
        for point in item.points.values()
        if point.lower_ns is not None
    ]
    uppers = [
        point.upper_ns
        for item in series
        for point in item.points.values()
        if point.upper_ns is not None
    ]
    min_y = min(values + lowers)
    max_y = max(values + uppers)
    min_x = min(sizes)
    max_x = max(sizes)
    title = " / ".join(part for part in [suite, workload, transport, variant] if part)

    def x_pos(size: int) -> float:
        if min_x == max_x:
            return (MARGIN_LEFT + WIDTH - MARGIN_RIGHT) / 2
        scale = (math.log(size) - math.log(min_x)) / (math.log(max_x) - math.log(min_x))
        return MARGIN_LEFT + scale * (WIDTH - MARGIN_LEFT - MARGIN_RIGHT)

    def y_pos(value: float) -> float:
        if min_y == max_y:
            return (MARGIN_TOP + HEIGHT - MARGIN_BOTTOM) / 2
        scale = (math.log(value) - math.log(min_y)) / (math.log(max_y) - math.log(min_y))
        return HEIGHT - MARGIN_BOTTOM - scale * (HEIGHT - MARGIN_TOP - MARGIN_BOTTOM)

    svg_parts = [
        f'<svg viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-label="{escape(title)} time chart">',
        render_axes(sizes, min_y, max_y, x_pos, y_pos),
    ]
    legend = []
    for idx, item in enumerate(series):
        color = COLORS[idx % len(COLORS)]
        present = sorted(item.points.values(), key=lambda point: point.key.size)
        if not present:
            continue
        path = " ".join(
            f"{'M' if n == 0 else 'L'} {x_pos(point.key.size):.2f} {y_pos(point.estimate_ns):.2f}"
            for n, point in enumerate(present)
        )
        svg_parts.append(f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.5"/>')
        for point in present:
            x = x_pos(point.key.size)
            y = y_pos(point.estimate_ns)
            svg_parts.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4" fill="{color}"/>')
            if point.lower_ns is not None and point.upper_ns is not None:
                y1 = y_pos(max(point.lower_ns, min_y))
                y2 = y_pos(min(point.upper_ns, max_y))
                svg_parts.append(
                    f'<line x1="{x:.2f}" y1="{y1:.2f}" x2="{x:.2f}" y2="{y2:.2f}" stroke="{color}" stroke-width="1.5" opacity="0.65"/>'
                )
        legend.append(
            f'<span><i class="swatch" style="background:{color}"></i>{escape(item.label)}</span>'
        )
    svg_parts.append("</svg>")

    return f"""<section class="chart">
<h2>{escape(title)}</h2>
{''.join(svg_parts)}
<div class="legend">{''.join(legend)}</div>
{render_ratio_table(series)}
</section>"""


def render_axes(sizes, min_y, max_y, x_pos, y_pos) -> str:
    left = MARGIN_LEFT
    right = WIDTH - MARGIN_RIGHT
    top = MARGIN_TOP
    bottom = HEIGHT - MARGIN_BOTTOM
    parts = [
        f'<line class="axis" x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}"/>',
        f'<line class="axis" x1="{left}" y1="{top}" x2="{left}" y2="{bottom}"/>',
        f'<text class="axis-label" x="{(left + right) / 2:.1f}" y="{HEIGHT - 14}" text-anchor="middle">message size, bytes</text>',
        f'<text class="axis-label" transform="translate(18 {(top + bottom) / 2:.1f}) rotate(-90)" text-anchor="middle">time, ns</text>',
    ]
    for size in sizes:
        x = x_pos(size)
        parts.append(f'<line class="grid" x1="{x:.2f}" y1="{top}" x2="{x:.2f}" y2="{bottom}"/>')
        parts.append(
            f'<text class="tick" x="{x:.2f}" y="{bottom + 20}" text-anchor="middle">{size}</text>'
        )

    for value in log_ticks(min_y, max_y):
        y = y_pos(value)
        parts.append(f'<line class="grid" x1="{left}" y1="{y:.2f}" x2="{right}" y2="{y:.2f}"/>')
        parts.append(
            f'<text class="tick" x="{left - 10}" y="{y + 4:.2f}" text-anchor="end">{format_ns(value)}</text>'
        )
    return "".join(parts)


def log_ticks(min_y: float, max_y: float) -> list[float]:
    start = math.floor(math.log10(min_y))
    end = math.ceil(math.log10(max_y))
    ticks = []
    for exponent in range(start, end + 1):
        for multiplier in (1, 2, 5):
            value = multiplier * 10**exponent
            if min_y <= value <= max_y:
                ticks.append(value)
    return ticks or [min_y, max_y]


def render_ratio_table(series: list[Series]) -> str:
    reference = next((item for item in series if item.label == "libzmq"), None)
    if reference is None:
        return ""

    compared = [item for item in series if item is not reference]
    if not compared:
        return ""

    common = sorted(
        {
            key
            for key in reference.points
            if any(key in item.points for item in compared)
        },
        key=lambda key: key.size,
    )
    if not common:
        return '<p class="missing">No overlapping libzmq data points for ratio calculation.</p>'

    headers = ["size", "libzmq"]
    for item in compared:
        headers.append(item.label)
        headers.append(f"{item.label} / libzmq")

    rows = []
    for key in common:
        cells = [str(key.size), format_ns(reference.points[key].estimate_ns)]
        for item in compared:
            point = item.points.get(key)
            if point is None:
                cells.extend(["-", "-"])
                continue
            ratio = point.estimate_ns / reference.points[key].estimate_ns
            cells.extend([format_ns(point.estimate_ns), f"{ratio:.2f}x"])
        rows.append("<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in cells) + "</tr>")

    head = "<tr>" + "".join(f"<th>{escape(item)}</th>" for item in headers) + "</tr>"
    return f"""<div class="table-wrap"><table>
<thead>{head}</thead>
<tbody>{''.join(rows)}</tbody>
</table></div>"""


def render_missing(series: list[Series]) -> str:
    missing = [item.label for item in series if not item.points]
    if not missing:
        return ""
    return (
        '<p class="missing">No data found for: '
        + ", ".join(escape(item) for item in missing)
        + ".</p>"
    )


def format_ns(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f} ms"
    if value >= 1_000:
        return f"{value / 1_000:.2f} us"
    return f"{value:.0f} ns"


def escape(value: object) -> str:
    return html.escape(str(value), quote=True)


if __name__ == "__main__":
    raise SystemExit(main())
