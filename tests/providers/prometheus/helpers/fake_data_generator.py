from typing import List, Dict
import time
from datetime import datetime, timedelta

import requests

EXPECTED_VALUES = {
    "test_counter": [
        (0, 90.0),  # Latest value for test_counter
        (1, 80.0),
        (2, 70.0),
        (3, 60.0),
        (4, 50.0),
        (5, 40.0),
        (6, 30.0),
        (7, 20.0),
        (8, 10.0),
        (9, 0.0),
    ],
    "test_gauge": [
        (0, 30.0),  # Latest value for test_gauge
        (1, 35.0),
        (2, 40.0),
        (3, 45.0),
        (4, 50.0),
    ],
}


def generate_test_metrics() -> List[Dict]:
    """Generate test metrics for Prometheus."""
    current_time = time.time() * 1000  # Prometheus expects milliseconds

    metrics = []

    # Counter metrics
    for i in range(10):
        metrics.append(
            {
                "timeseries": [
                    {
                        "labels": [
                            {"name": "__name__", "value": "test_counter"},
                            {"name": "instance", "value": "test1"},
                            {"name": "job", "value": "test_job"},
                        ],
                        "samples": [
                            {
                                "timestamp": current_time - (i * 60 * 1000),
                                "value": float(i * 10),
                            }
                        ],
                    }
                ]
            }
        )

    # Gauge metrics
    for i in range(5):
        metrics.append(
            {
                "timeseries": [
                    {
                        "labels": [
                            {"name": "__name__", "value": "test_gauge"},
                            {"name": "instance", "value": "test1"},
                            {"name": "job", "value": "test_job"},
                        ],
                        "samples": [
                            {
                                "timestamp": current_time - (i * 60 * 1000),
                                "value": float(50 - i * 5),
                            }
                        ],
                    }
                ]
            }
        )

    # Histogram metrics
    buckets = [0.1, 0.5, 1, 2.5, 5, 10]
    for bucket in buckets:
        metrics.append(
            {
                "timeseries": [
                    {
                        "labels": [
                            {"name": "__name__", "value": "test_histogram_bucket"},
                            {"name": "instance", "value": "test1"},
                            {"name": "job", "value": "test_job"},
                            {"name": "le", "value": str(bucket)},
                        ],
                        "samples": [
                            {"timestamp": current_time, "value": float(bucket * 100)}
                        ],
                    }
                ]
            }
        )

    # High cardinality metrics
    for i in range(100):
        metrics.append(
            {
                "timeseries": [
                    {
                        "labels": [
                            {"name": "__name__", "value": "test_high_cardinality"},
                            {"name": "instance", "value": f"test{i}"},
                            {"name": "job", "value": "test_job"},
                            {"name": "id", "value": str(i)},
                        ],
                        "samples": [{"timestamp": current_time, "value": float(i)}],
                    }
                ]
            }
        )

    return metrics


def write_metrics_to_pushgateway(url: str, metrics: List[Dict]) -> None:
    """Write metrics to Pushgateway in the correct format."""
    # Group metrics by job and instance
    grouped_metrics = {}
    seen_types = set()  # Track which metrics we've already written TYPE for

    for metric_data in metrics:
        for timeseries in metric_data["timeseries"]:
            labels = {label["name"]: label["value"] for label in timeseries["labels"]}
            metric_name = labels.pop("__name__", "unknown_metric")
            instance = labels.pop("instance", "default")
            job = labels.pop("job", "test_job")

            key = (job, instance)
            if key not in grouped_metrics:
                grouped_metrics[key] = []

            # Determine metric type based on name
            metric_type = None
            if metric_name.endswith("_total") or metric_name == "test_counter":
                metric_type = "counter"
            elif metric_name == "test_gauge":
                metric_type = "gauge"
            elif metric_name.endswith("_bucket"):
                metric_type = "histogram"

            # Add TYPE hint only once per metric name
            if metric_type and metric_name not in seen_types:
                grouped_metrics[key].append(f"# TYPE {metric_name} {metric_type}\n")
                seen_types.add(metric_name)

            # Format the metric line without TYPE hint
            label_pairs = []
            for k, v in sorted(labels.items()):
                escaped_value = str(v).replace('"', '\\"')
                label_pairs.append(f'{k}="{escaped_value}"')
            label_str = "{" + ",".join(label_pairs) + "}" if labels else ""

            # Add the metric line
            metric_line = f"{metric_name}{label_str} {float(timeseries['samples'][-1]['value'])}\n"
            grouped_metrics[key].append(metric_line)

    # Write metrics grouped by job and instance
    for (job, instance), metrics_lines in grouped_metrics.items():
        # First delete any existing metrics for this job/instance
        requests.delete(f"{url}/metrics/job/{job}/instance/{instance}")

        # Then write the new metrics
        response = requests.post(
            f"{url}/metrics/job/{job}/instance/{instance}",
            data="".join(metrics_lines),
            headers={"Content-Type": "text/plain"},
        )
        if response.status_code != 200:
            raise Exception(f"Failed to write metrics: {response.text}")


def verify_data_present(client):
    """Verify test data is present and matches expected values."""
    result = client.query("test_counter")
    if not result.metrics:
        return False

    counter_value = result.metrics[0].latest_value
    expected_value = EXPECTED_VALUES["test_counter"][0][1]  # Latest value
    return (
        abs(counter_value - expected_value) < 0.1
    )  # Allow small floating point difference
