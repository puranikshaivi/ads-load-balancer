import argparse
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


SUBMISSION_FILES = [
    "README.md",
    "PHASE3_REPORT.md",
    "Dockerfile",
    "docker-compose.yml",
    "requirements.txt",
    "phase2_results.csv",
    "phase2_results_legacy.csv",
    "client/analytics.py",
    "client/client.py",
    "server/server.py",
    "server/texts/pride-and-prejudice.txt",
    "load_balancer/proxy.py",
    "experiments/benchmark.py",
    "experiments/export_proxy_timings.py",
    "experiments/export_results.py",
    "experiments/package_submission.py",
    "experiments/plot_results.py",
    "experiments/rate_benchmark.py",
    "tests/__init__.py",
    "tests/test_export_proxy_timings.py",
    "tests/test_load_balancer.py",
    "tests/test_rate_benchmark.py",
    "docs/phase3-architecture.svg",
    "results/phase2-warm-cache-verified.json",
    "results/phase2-compute-miss-verified.json",
    "results/phase3-least-connections-warm-cache.json",
    "results/phase3-least-connections-compute-miss.json",
    "results/phase3-least-response-time-warm-cache.json",
    "results/phase3-least-response-time-compute-miss.json",
    "results/phase3_results.csv",
    "results/phase3-lrt-backend-timings.csv",
    "results/phase3-lrt-proxy.log",
    "results/phase3-warm-cache-average-latency.svg",
    "results/phase3-warm-cache-p99-latency.svg",
    "results/phase3-warm-cache-replica-distribution.svg",
    "results/phase3-compute-miss-average-latency.svg",
    "results/phase3-compute-miss-p99-latency.svg",
    "results/phase3-compute-miss-replica-distribution.svg",
]


def build_submission(root, output_path):
    root = Path(root)
    output_path = Path(output_path)
    paths = [root / relative_path for relative_path in SUBMISSION_FILES]
    missing = [
        path.relative_to(root).as_posix()
        for path in paths
        if not path.is_file()
    ]
    if missing:
        raise FileNotFoundError(
            "submission is missing required files: " + ", ".join(missing)
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output_path, "w", compression=ZIP_DEFLATED) as archive:
        for path in paths:
            archive.write(path, path.relative_to(root).as_posix())
    with ZipFile(output_path) as archive:
        corrupted_file = archive.testzip()
        if corrupted_file:
            raise OSError(f"corrupt file in submission archive: {corrupted_file}")
        expected = set(SUBMISSION_FILES)
        actual = set(archive.namelist())
        if actual != expected:
            raise OSError("submission archive contents do not match the allowlist")
    return len(paths), output_path.stat().st_size


def main():
    parser = argparse.ArgumentParser(
        description="Build a clean Phase 3 submission archive from an allowlist."
    )
    parser.add_argument(
        "--output",
        default="Phase3_Submission.zip",
        help="archive path (default: Phase3_Submission.zip)",
    )
    args = parser.parse_args()

    count, size = build_submission(Path(__file__).resolve().parents[1], args.output)
    print(f"Created {args.output}: {count} files, {size} bytes")


if __name__ == "__main__":
    main()
