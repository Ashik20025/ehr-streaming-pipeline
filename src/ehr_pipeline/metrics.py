from __future__ import annotations

import statistics
import time
from collections import Counter
from dataclasses import dataclass, field


@dataclass
class Metrics:
    start: float = field(default_factory=time.perf_counter)
    total: int = 0
    accepted: int = 0
    quarantined: int = 0
    duplicates: int = 0
    late: int = 0
    out_of_order: int = 0
    alerts: int = 0
    reasons: Counter[str] = field(default_factory=Counter)
    latency_ms: list[float] = field(default_factory=list)
    bucket_size: int = 1000
    _bucket_start: float = field(default_factory=time.perf_counter)
    _bucket_count: int = 0
    throughput_buckets: list[float] = field(default_factory=list)

    def observe_event(self) -> None:
        self.total += 1
        self._bucket_count += 1
        if self._bucket_count >= self.bucket_size:
            now = time.perf_counter()
            elapsed = max(now - self._bucket_start, 1e-9)
            self.throughput_buckets.append(self._bucket_count / elapsed)
            self._bucket_start = now
            self._bucket_count = 0

    def observe_latency(self, value_ms: float) -> None:
        self.latency_ms.append(value_ms)

    def finish(self) -> dict[str, float | int | dict[str, int] | list[float]]:
        if self._bucket_count:
            now = time.perf_counter()
            elapsed = max(now - self._bucket_start, 1e-9)
            self.throughput_buckets.append(self._bucket_count / elapsed)
            self._bucket_count = 0

        duration = max(time.perf_counter() - self.start, 1e-9)
        latencies = sorted(self.latency_ms)
        return {
            "total": self.total,
            "accepted": self.accepted,
            "quarantined": self.quarantined,
            "duplicates": self.duplicates,
            "late": self.late,
            "out_of_order": self.out_of_order,
            "alerts": self.alerts,
            "duration_sec": duration,
            "throughput_eps": self.total / duration,
            "p50_latency_ms": _percentile(latencies, 50),
            "p95_latency_ms": _percentile(latencies, 95),
            "mean_latency_ms": statistics.fmean(latencies) if latencies else 0.0,
            "reasons": dict(self.reasons),
            "throughput_buckets": self.throughput_buckets,
        }


def _percentile(values: list[float], percentile: int) -> float:
    if not values:
        return 0.0
    index = min(len(values) - 1, round((percentile / 100) * (len(values) - 1)))
    return values[index]

