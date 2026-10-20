import argparse
import html
import json
from pathlib import Path


WIDTH = 960
HEIGHT = 560
LEFT = 90
RIGHT = 245
TOP = 70
BOTTOM = 85
COLORS = ["#2563eb", "#dc2626", "#16a34a"]


def read_results(paths):
    results = []
    for path in paths:
        with Path(path).open(encoding="utf-8") as result_file:
            results.append(json.load(result_file))
    if not results:
        raise ValueError("provide at least one result JSON file")
    return results


def svg_text(x, y, text, size=14, anchor="middle", weight="normal"):
    return (
        f'<text x="{x}" y="{y}" font-family="Arial, sans-serif" '
        f'font-size="{size}" text-anchor="{anchor}" font-weight="{weight}">'
        f"{html.escape(str(text))}</text>"
    )


def line_chart(results, metric, ylabel, title, output_path):
    rates = sorted(
        {
            row["offered_rate_requests_per_second"]
            for result in results
            for row in result["rates"]
        }
    )
    values = [
        row[metric]
        for result in results
        for row in result["rates"]
    ]
    max_value = max(values) if values else 1
    y_max = max(1, int((max_value * 1.15 + 9) // 10 * 10))
    plot_width = WIDTH - LEFT - RIGHT
    plot_height = HEIGHT - TOP - BOTTOM
    x_positions = {
        rate: LEFT + (index / max(1, len(rates) - 1)) * plot_width
        for index, rate in enumerate(rates)
    }

    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}">',
        '<rect width="100%" height="100%" fill="white"/>',
        svg_text(WIDTH / 2, 32, title, 20, weight="bold"),
    ]
    for tick in range(6):
        value = y_max * tick / 5
        y = TOP + plot_height * (1 - tick / 5)
        elements.append(
            f'<line x1="{LEFT}" y1="{y:.1f}" x2="{LEFT + plot_width}" '
            f'y2="{y:.1f}" stroke="#d1d5db"/>'
        )
        elements.append(svg_text(LEFT - 12, y + 5, f"{value:.0f}", 12, "end"))
    for rate in rates:
        x = x_positions[rate]
        elements.append(
            f'<line x1="{x:.1f}" y1="{TOP}" x2="{x:.1f}" '
            f'y2="{TOP + plot_height}" stroke="#f3f4f6"/>'
        )
        elements.append(svg_text(x, TOP + plot_height + 24, rate, 12))

    elements.extend(
        [
            f'<line x1="{LEFT}" y1="{TOP}" x2="{LEFT}" '
            f'y2="{TOP + plot_height}" stroke="#111827"/>',
            f'<line x1="{LEFT}" y1="{TOP + plot_height}" '
            f'x2="{LEFT + plot_width}" y2="{TOP + plot_height}" stroke="#111827"/>',
            f'<text x="22" y="{TOP + plot_height / 2}" '
            f'transform="rotate(-90 22 {TOP + plot_height / 2})" '
            'font-family="Arial, sans-serif" font-size="14" text-anchor="middle">'
            f"{html.escape(ylabel)}</text>",
            svg_text(LEFT + plot_width / 2, HEIGHT - 22, "Offered request rate (requests/s)", 14),
        ]
    )

    for result_index, result in enumerate(results):
        color = COLORS[result_index % len(COLORS)]
        label = result["label"]
        coordinates = []
        for row in result["rates"]:
            x = x_positions[row["offered_rate_requests_per_second"]]
            y = TOP + plot_height * (1 - row[metric] / y_max)
            coordinates.append(f"{x:.1f},{y:.1f}")
            elements.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5" fill="{color}"/>'
            )
        elements.append(
            f'<polyline points="{" ".join(coordinates)}" fill="none" '
            f'stroke="{color}" stroke-width="2.5"/>'
        )
        legend_y = TOP + result_index * 26
        elements.append(
            f'<line x1="{LEFT + plot_width + 28}" y1="{legend_y}" '
            f'x2="{LEFT + plot_width + 54}" y2="{legend_y}" '
            f'stroke="{color}" stroke-width="3"/>'
        )
        elements.append(
            svg_text(LEFT + plot_width + 62, legend_y + 5, label, 12, "start")
        )

    elements.append("</svg>")
    Path(output_path).write_text("\n".join(elements) + "\n", encoding="utf-8")


def distribution_chart(results, output_path):
    server_names = sorted(
        {
            server
            for result in results
            for row in result["rates"]
            for server in row["servers"]
        }
    )
    totals = {
        result["label"]: {
            server: sum(row["servers"].get(server, 0) for row in result["rates"])
            for server in server_names
        }
        for result in results
    }
    maximum = max(
        (count for servers in totals.values() for count in servers.values()),
        default=1,
    )
    y_max = max(1, int((maximum * 1.15 + 9) // 10 * 10))
    plot_width = WIDTH - LEFT - RIGHT
    plot_height = HEIGHT - TOP - BOTTOM
    group_count = len(results) * len(server_names)
    slot_width = plot_width / max(1, group_count)
    bar_width = slot_width * 0.68

    elements = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
        f'viewBox="0 0 {WIDTH} {HEIGHT}">',
        '<rect width="100%" height="100%" fill="white"/>',
        svg_text(WIDTH / 2, 32, "Requests handled by server (all tested rates)", 20, weight="bold"),
    ]
    for tick in range(6):
        value = y_max * tick / 5
        y = TOP + plot_height * (1 - tick / 5)
        elements.append(
            f'<line x1="{LEFT}" y1="{y:.1f}" x2="{LEFT + plot_width}" '
            f'y2="{y:.1f}" stroke="#d1d5db"/>'
        )
        elements.append(svg_text(LEFT - 12, y + 5, f"{value:.0f}", 12, "end"))

    elements.extend(
        [
            f'<line x1="{LEFT}" y1="{TOP}" x2="{LEFT}" '
            f'y2="{TOP + plot_height}" stroke="#111827"/>',
            f'<line x1="{LEFT}" y1="{TOP + plot_height}" '
            f'x2="{LEFT + plot_width}" y2="{TOP + plot_height}" stroke="#111827"/>',
            f'<text x="22" y="{TOP + plot_height / 2}" '
            f'transform="rotate(-90 22 {TOP + plot_height / 2})" '
            'font-family="Arial, sans-serif" font-size="14" text-anchor="middle">'
            "Successful requests</text>",
        ]
    )

    index = 0
    for result_index, result in enumerate(results):
        for server in server_names:
            count = totals[result["label"]][server]
            x = LEFT + index * slot_width + (slot_width - bar_width) / 2
            bar_height = plot_height * count / y_max
            y = TOP + plot_height - bar_height
            color = COLORS[result_index % len(COLORS)]
            elements.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" '
                f'height="{bar_height:.1f}" fill="{color}"/>'
            )
            elements.append(svg_text(x + bar_width / 2, y - 6, count, 10))
            elements.append(
                svg_text(x + bar_width / 2, TOP + plot_height + 20, server, 10)
            )
            index += 1

    for result_index, result in enumerate(results):
        color = COLORS[result_index % len(COLORS)]
        legend_y = TOP + result_index * 24
        elements.append(
            f'<rect x="{LEFT + plot_width + 26}" y="{legend_y - 8}" '
            f'width="14" height="14" fill="{color}"/>'
        )
        elements.append(
            svg_text(LEFT + plot_width + 48, legend_y + 4, result["label"], 12, "start")
        )
    elements.append("</svg>")
    Path(output_path).write_text("\n".join(elements) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(
        description="Create SVG latency and replica-distribution charts."
    )
    parser.add_argument("results", nargs="+", help="per-configuration JSON files")
    parser.add_argument("--output-dir", default="/app/results")
    parser.add_argument("--prefix", default="phase3")
    args = parser.parse_args()

    results = read_results(args.results)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    line_chart(
        results,
        "average_latency_ms",
        "Average latency (ms)",
        "Average client-observed latency",
        output_dir / f"{args.prefix}-average-latency.svg",
    )
    line_chart(
        results,
        "p99_latency_ms",
        "P99 latency (ms)",
        "99th-percentile client-observed latency",
        output_dir / f"{args.prefix}-p99-latency.svg",
    )
    distribution_chart(
        results,
        output_dir / f"{args.prefix}-replica-distribution.svg",
    )
    print(f"SVG charts saved to {output_dir}")


if __name__ == "__main__":
    main()
