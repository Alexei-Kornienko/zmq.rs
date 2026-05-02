#!/usr/bin/env python3
"""Generate comparison reports from Criterion benchmark JSON output."""

from __future__ import annotations

import argparse
import html
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


COLORS = ["#2563eb", "#dc2626", "#059669", "#9333ea"]
BASIC_PATTERNS = {"pub_sub", "req_rep", "push_pull", "dealer_router"}
WIDTH = 840
HEIGHT = 340
MARGIN_LEFT = 76
MARGIN_RIGHT = 24
MARGIN_TOP = 30
MARGIN_BOTTOM = 58


@dataclass(frozen=True)
class BenchKey:
    pattern: str
    transport: str
    variant: str
    size: int


@dataclass(frozen=True)
class BenchPoint:
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
    criterion_dir = Path(args.criterion_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    all_points = load_points(criterion_dir)
    baselines = {point.baseline for point in all_points}
    tokio_baseline = choose_baseline(args.tokio_baseline, baselines, fallback="new")
    async_std_baseline = choose_baseline(args.async_std_baseline, baselines, fallback=None)

    reports = [
        (
            "libzmq-vs-zmqrs-tokio.html",
            "libzmq vs zmqrs (tokio runtime)",
            [
                select_series(all_points, "libzmq", tokio_baseline, "libzmq"),
                select_series(all_points, "zmqrs", tokio_baseline, "zmqrs tokio"),
            ],
            f"Baseline: {tokio_baseline}",
        ),
        (
            "libzmq-vs-zmqrs-async-std.html",
            "libzmq vs zmqrs (async-std runtime)",
            [
                select_series(all_points, "libzmq", async_std_baseline, "libzmq"),
                select_series(all_points, "zmqrs", async_std_baseline, "zmqrs async-std"),
            ],
            f"Baseline: {async_std_baseline}",
        ),
        (
            "zmqrs-tokio-vs-async-std.html",
            "zmqrs tokio vs zmqrs async-std",
            [
                select_series(all_points, "zmqrs", tokio_baseline, "zmqrs tokio"),
                select_series(all_points, "zmqrs", async_std_baseline, "zmqrs async-std"),
            ],
            f"Baselines: tokio={tokio_baseline}, async-std={async_std_baseline}",
        ),
    ]

    written = []
    for file_name, title, series, subtitle in reports:
        output = out_dir / file_name
        output.write_text(render_report(title, subtitle, series), encoding="utf-8")
        written.append(output)

    print(f"Loaded {len(all_points)} Criterion measurements from {criterion_dir}")
    print(f"Available baselines: {', '.join(sorted(baselines)) or '(none)'}")
    for path in written:
        print(f"Wrote {path}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate standalone HTML comparison graphs from Criterion JSON data."
    )
    parser.add_argument("--criterion-dir", default="target/criterion")
    parser.add_argument("--out-dir", default="target/criterion-comparison")
    parser.add_argument("--tokio-baseline", default="tokio")
    parser.add_argument("--async-std-baseline", default="async-std")
    return parser.parse_args()


def choose_baseline(preferred: str, available: set[str], fallback: str | None) -> str:
    if preferred in available:
        return preferred
    if fallback is not None and fallback in available:
        return fallback
    return preferred


def load_points(criterion_dir: Path) -> list[BenchPoint]:
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

        point = parse_point(benchmark_path.parent.name, benchmark, estimates)
        if point is not None:
            points.append(point)
    return points


def parse_point(baseline: str, benchmark: dict, estimates: dict) -> BenchPoint | None:
    group_id = benchmark.get("group_id")
    full_id = benchmark.get("full_id")
    value_str = benchmark.get("value_str")
    if not isinstance(group_id, str) or not isinstance(full_id, str):
        return None

    parts = group_id.split("/")
    if len(parts) < 3 or parts[0] not in {"libzmq", "zmqrs"}:
        return None
    if parts[1] not in BASIC_PATTERNS:
        return None

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

    return BenchPoint(
        impl=parts[0],
        baseline=baseline,
        key=BenchKey(
            pattern=parts[1],
            transport=parts[2],
            variant="/".join(parts[3:]),
            size=size,
        ),
        full_id=full_id,
        estimate_ns=float(point_estimate),
        lower_ns=float(lower) if isinstance(lower, (int, float)) else None,
        upper_ns=float(upper) if isinstance(upper, (int, float)) else None,
        throughput_bytes=int(throughput_bytes) if isinstance(throughput_bytes, int) else None,
    )


def select_series(
    points: Iterable[BenchPoint], impl: str, baseline: str, label: str
) -> Series:
    selected = {
        point.key: point
        for point in points
        if point.impl == impl and point.baseline == baseline
    }
    return Series(label=label, points=selected)


def render_report(title: str, subtitle: str, series: list[Series]) -> str:
    chart_keys = sorted(
        {
            (key.pattern, key.transport, key.variant)
            for item in series
            for key in item.points
        }
    )
    charts = []
    for pattern, transport, variant in chart_keys:
        keys = sorted(
            {
                key
                for item in series
                for key in item.points
                if key.pattern == pattern
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
            charts.append(render_chart(pattern, transport, variant, chart_series))

    missing = render_missing(series)
    body = "\n".join(charts) if charts else "<p class=\"empty\">No matching measurements found.</p>"
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
  max-width: 1100px;
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
table {{
  margin-top: 12px;
  border-collapse: collapse;
  font-size: 13px;
}}
th, td {{
  padding: 6px 10px;
  border-bottom: 1px solid #e5eaf2;
  text-align: right;
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
<p class="subtitle">{escape(subtitle)}. Lower latency is better. Points use Criterion slope estimates in ns with 95% confidence intervals.</p>
{body}
{missing}
</main>
</body>
</html>
"""


def render_chart(pattern: str, transport: str, variant: str, series: list[Series]) -> str:
    sizes = sorted({key.size for item in series for key in item.points})
    values = [point.estimate_ns for item in series for point in item.points.values()]
    lowers = [
        point.lower_ns for item in series for point in item.points.values() if point.lower_ns
    ]
    uppers = [
        point.upper_ns for item in series for point in item.points.values() if point.upper_ns
    ]
    min_y = min(values + lowers)
    max_y = max(values + uppers)
    min_x = min(sizes)
    max_x = max(sizes)
    title = " ".join(part for part in [pattern, transport, variant] if part)

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
        f'<svg viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-label="{escape(title)} latency chart">',
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
        f'<text class="axis-label" transform="translate(18 {(top + bottom) / 2:.1f}) rotate(-90)" text-anchor="middle">latency, ns</text>',
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
    if len(series) < 2:
        return ""
    left, right = series[0], series[1]
    common = sorted(set(left.points) & set(right.points), key=lambda key: key.size)
    if not common:
        return '<p class="missing">No overlapping data points for ratio calculation.</p>'

    rows = []
    for key in common:
        left_ns = left.points[key].estimate_ns
        right_ns = right.points[key].estimate_ns
        ratio = right_ns / left_ns
        faster = left.label if ratio > 1 else right.label
        rows.append(
            f"<tr><td>{key.size}</td><td>{format_ns(left_ns)}</td><td>{format_ns(right_ns)}</td><td>{ratio:.2f}x</td><td>{escape(faster)}</td></tr>"
        )
    return f"""<table>
<thead><tr><th>size</th><th>{escape(left.label)}</th><th>{escape(right.label)}</th><th>{escape(right.label)} / {escape(left.label)}</th><th>lower latency</th></tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>"""


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
