from typing import Any, Dict, List

from rated_exporter_sdk.providers.prometheus.types import (
    MetricType,
    PrometheusMetric,
    Sample,
)


def validate_metric_samples(samples: List[Sample]) -> bool:
    """Validate metric samples are properly formatted and ordered."""
    if not samples:
        return False

    # Verify timestamps are ordered
    timestamps = [s.timestamp for s in samples]
    return all(t1 <= t2 for t1, t2 in zip(timestamps, timestamps[1:]))


def validate_histogram_buckets(metrics: List[PrometheusMetric]) -> bool:
    """Validate histogram buckets are properly ordered and cumulative."""
    if not metrics:
        return False

    # Extract bucket values and verify they're cumulative
    buckets = {
        float(m.identifier.labels["le"]): m.latest_value
        for m in metrics
        if "le" in m.identifier.labels
    }

    values = list(buckets.values())
    return all(v1 <= v2 for v1, v2 in zip(values, values[1:]))


def validate_query_result(result: Dict[str, Any], expected_type: MetricType) -> bool:
    """Validate a query result matches expected format and type."""
    if not result.get("status") == "success":
        return False

    data = result.get("data", {})
    if data.get("resultType") != expected_type.value:
        return False

    return "result" in data
