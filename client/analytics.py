import json
import statistics
from datetime import datetime, timezone
from pathlib import Path


def build_report(file_path, results):
    successful = [result for result in results if result["ok"]]
    latencies = [result["latency_ms"] for result in results]
    cache_hits = sum(result["result"]["cache_status"] == "HIT" for result in successful)
    cache_misses = sum(result["result"]["cache_status"] == "MISS" for result in successful)

    return {
        "file": file_path,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "requests": len(results),
            "successful_requests": len(successful),
            "failed_servers": len(results) - len(successful),
            "average_latency_ms": round(statistics.mean(latencies), 2) if latencies else 0,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
        },
        "results": results,
    }


def save_report(report, output_path):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")