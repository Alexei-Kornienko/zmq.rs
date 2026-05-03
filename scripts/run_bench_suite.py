#!/usr/bin/env python3
"""Run libzmq and zmqrs runtime benchmarks as one isolated benchmark suite."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 fallback.
    tomllib = None  # type: ignore[assignment]


DEFAULT_BENCHES = ["compare_libzmq", "throughput"]
PREFERRED_RUNTIME_ORDER = [
    "tokio-runtime",
    "async-std-runtime",
    "async-dispatcher-runtime",
    "monoio-runtime",
]


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    cargo_toml = repo_root / "Cargo.toml"
    features = load_features(cargo_toml)
    all_runtimes = discover_runtime_features(features)
    selected_runtimes = select_runtimes(args.runtimes, all_runtimes)
    reference_runtime = normalize_runtime(args.reference_runtime)
    if reference_runtime not in all_runtimes:
        raise SystemExit(
            f"reference runtime {reference_runtime!r} is not a Cargo feature ending in '-runtime'"
        )

    benches = args.benches or DEFAULT_BENCHES
    target_dir = (repo_root / args.target_dir).resolve()
    criterion_dir = target_dir / "criterion"
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = (repo_root / args.out_root / run_id).resolve()
    run_dir.mkdir(parents=True, exist_ok=False)

    manifest: dict[str, Any] = {
        "schema": 1,
        "status": "running",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(repo_root),
        "target_dir": str(target_dir),
        "criterion_dir": str(criterion_dir),
        "sample_size": args.sample_size,
        "benches": benches,
        "extra_criterion_args": args.criterion_arg,
        "reference_runtime": reference_runtime,
        "entries": [],
    }
    maybe_add_git_revision(repo_root, manifest)
    write_manifest(run_dir, manifest)

    try:
        reference_entry = run_entry(
            args=args,
            repo_root=repo_root,
            run_dir=run_dir,
            criterion_dir=criterion_dir,
            benches=benches,
            name="libzmq",
            kind="reference",
            impl_filter="libzmq",
            runtime_feature=reference_runtime,
            cargo_features=bench_features(features, reference_runtime),
        )
        manifest["entries"].append(reference_entry)
        write_manifest(run_dir, manifest)

        for runtime_feature in selected_runtimes:
            runtime_name = runtime_label(runtime_feature)
            runtime_entry = run_entry(
                args=args,
                repo_root=repo_root,
                run_dir=run_dir,
                criterion_dir=criterion_dir,
                benches=benches,
                name=runtime_name,
                kind="runtime",
                impl_filter="zmqrs",
                runtime_feature=runtime_feature,
                cargo_features=bench_features(features, runtime_feature),
            )
            manifest["entries"].append(runtime_entry)
            write_manifest(run_dir, manifest)

        report_dir = run_dir / "report"
        graph_cmd = [
            sys.executable,
            str(repo_root / "scripts" / "criterion_graphs.py"),
            "--run-dir",
            str(run_dir),
            "--out-dir",
            str(report_dir),
        ]
        run_command(graph_cmd, repo_root, dry_run=args.dry_run)

        manifest["status"] = "complete"
        manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
        manifest["report_dir"] = str(report_dir.relative_to(run_dir))
        write_manifest(run_dir, manifest)
        print(f"Benchmark run complete: {run_dir}")
        print(f"Report directory: {report_dir}")
        return 0
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["failed_at"] = datetime.now(timezone.utc).isoformat()
        manifest["error"] = str(exc)
        write_manifest(run_dir, manifest)
        print(f"Benchmark run failed: {exc}", file=sys.stderr)
        print(f"Partial run directory: {run_dir}", file=sys.stderr)
        return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run libzmq once, run every zmqrs runtime, then plot one comparison report."
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--target-dir", default="target")
    parser.add_argument("--out-root", type=Path, default=Path("target/bench-runs"))
    parser.add_argument("--run-id")
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--benches", nargs="*", default=DEFAULT_BENCHES)
    parser.add_argument(
        "--runtimes",
        nargs="*",
        help="Runtime labels or feature names. Defaults to every Cargo feature ending in '-runtime'.",
    )
    parser.add_argument(
        "--reference-runtime",
        default="tokio-runtime",
        help="Runtime feature used only to compile the libzmq reference bench binary.",
    )
    parser.add_argument(
        "--criterion-arg",
        action="append",
        default=[],
        help="Additional argument passed to Criterion after --. Repeat for multiple args.",
    )
    parser.add_argument("--cargo", default="cargo")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands and create metadata without running cargo or plotting.",
    )
    return parser.parse_args()


def load_features(cargo_toml: Path) -> dict[str, list[str]]:
    if tomllib is None:
        raise SystemExit("Python 3.11+ is required so scripts can parse Cargo.toml with tomllib")
    try:
        data = tomllib.loads(cargo_toml.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SystemExit(f"failed to read {cargo_toml}: {exc}") from exc
    features = data.get("features")
    if not isinstance(features, dict):
        raise SystemExit(f"{cargo_toml} does not define a [features] table")
    return {
        str(name): list(values) if isinstance(values, list) else []
        for name, values in features.items()
    }


def discover_runtime_features(features: dict[str, list[str]]) -> list[str]:
    runtimes = [feature for feature in features if feature.endswith("-runtime")]
    preferred = [feature for feature in PREFERRED_RUNTIME_ORDER if feature in runtimes]
    remaining = sorted(feature for feature in runtimes if feature not in preferred)
    if not preferred and not remaining:
        raise SystemExit("no Cargo features ending in '-runtime' were found")
    return preferred + remaining


def select_runtimes(requested: list[str] | None, available: list[str]) -> list[str]:
    if not requested:
        return available
    selected = [normalize_runtime(item) for item in requested]
    unknown = [item for item in selected if item not in available]
    if unknown:
        raise SystemExit(
            "unknown runtime feature(s): "
            + ", ".join(unknown)
            + f". Available: {', '.join(available)}"
        )
    return selected


def normalize_runtime(value: str) -> str:
    return value if value.endswith("-runtime") else f"{value}-runtime"


def runtime_label(runtime_feature: str) -> str:
    return runtime_feature.removesuffix("-runtime")


def bench_features(all_features: dict[str, list[str]], runtime_feature: str) -> list[str]:
    features = [runtime_feature]
    if "all-transport" in all_features:
        features.append("all-transport")
    return features


def run_entry(
    *,
    args: argparse.Namespace,
    repo_root: Path,
    run_dir: Path,
    criterion_dir: Path,
    benches: list[str],
    name: str,
    kind: str,
    impl_filter: str,
    runtime_feature: str,
    cargo_features: list[str],
) -> dict[str, Any]:
    entry_dir = run_dir / safe_path_part(name)
    entry_dir.mkdir(parents=True, exist_ok=False)
    bench_results = []
    commands = []

    for bench in benches:
        if criterion_dir.exists():
            shutil.rmtree(criterion_dir)

        baseline = safe_path_part(name)
        cmd = [
            args.cargo,
            "bench",
            "--no-default-features",
            "--features",
            ",".join(cargo_features),
            "--bench",
            bench,
            "--",
            "--sample-size",
            str(args.sample_size),
            "--save-baseline",
            baseline,
            *args.criterion_arg,
            impl_filter,
        ]
        commands.append(cmd)
        run_command(cmd, repo_root, dry_run=args.dry_run)

        bench_dest = entry_dir / bench / "criterion"
        if args.dry_run:
            bench_dest.mkdir(parents=True, exist_ok=True)
        elif criterion_dir.exists():
            shutil.copytree(criterion_dir, bench_dest)
        else:
            raise RuntimeError(f"{bench} did not produce {criterion_dir}")

        bench_results.append(
            {
                "name": bench,
                "criterion_dir": str(bench_dest.relative_to(run_dir)),
            }
        )

    return {
        "name": name,
        "kind": kind,
        "impl": impl_filter,
        "runtime_feature": runtime_feature,
        "cargo_features": cargo_features,
        "benches": bench_results,
        "commands": commands,
    }


def run_command(cmd: list[str], cwd: Path, *, dry_run: bool) -> None:
    print("+ " + " ".join(cmd))
    if dry_run:
        return
    subprocess.run(cmd, cwd=cwd, check=True)


def write_manifest(run_dir: Path, manifest: dict[str, Any]) -> None:
    path = run_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def maybe_add_git_revision(repo_root: Path, manifest: dict[str, Any]) -> None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return
    manifest["git_revision"] = result.stdout.strip()


def safe_path_part(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip())
    return sanitized.strip(".-") or "unnamed"


if __name__ == "__main__":
    raise SystemExit(main())
