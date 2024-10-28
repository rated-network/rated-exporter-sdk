import time
from typing import Dict, List

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


def generate_test_metrics(
    num_samples: int = 5, interval_minutes: int = 15
) -> List[Dict]:
    """Generate test metrics for Prometheus with multiple timestamps.

    Args:
        num_samples: Number of time series samples to generate
        interval_minutes: Minutes between samples

    Returns:
        List of metric dictionaries with multiple timestamps
    """
    metrics = []
    now = time.time()

    # Generate samples for multiple timestamps
    for sample_idx in range(num_samples):
        # Go backwards in time, with interval_minutes intervals
        timestamp = int((now - (sample_idx * interval_minutes * 60)) * 1000)

        # Counter metrics - using different instances
        for i in range(10):
            base_value = 90.0 - (i * 10)  # Start with original value scheme
            # Add some time-based variation - decreasing over time
            time_adjusted_value = base_value - (sample_idx * 2)

            metrics.append(
                {
                    "timeseries": [
                        {
                            "labels": [
                                {"name": "__name__", "value": "test_counter"},
                                {"name": "instance", "value": f"counter_instance_{i}"},
                                {"name": "job", "value": "test_job"},
                            ],
                            "samples": [
                                {
                                    "timestamp": timestamp,
                                    "value": float(time_adjusted_value),
                                }
                            ],
                        }
                    ]
                }
            )

        # Gauge metrics - using different instances
        for i in range(5):
            base_value = 30.0 + (i * 5)
            # Add some time-based variation - oscillating pattern
            time_adjusted_value = base_value + (
                sample_idx * 3 * (-1 if i % 2 == 0 else 1)
            )

            metrics.append(
                {
                    "timeseries": [
                        {
                            "labels": [
                                {"name": "__name__", "value": "test_gauge"},
                                {"name": "instance", "value": f"gauge_instance_{i}"},
                                {"name": "job", "value": "test_job"},
                            ],
                            "samples": [
                                {
                                    "timestamp": timestamp,
                                    "value": float(time_adjusted_value),
                                }
                            ],
                        }
                    ]
                }
            )

        # Histogram metrics with realistic distributions
        buckets = [0.1, 0.5, 1.0, 2.5, 5.0, 10.0, float("inf")]
        cumulative_counts = []
        total_count = 1000 + (sample_idx * 100)  # Increasing total count over time
        bucket_values = [
            100,
            250,
            400,
            600,
            800,
            950,
            1000,
        ]  # Representative distribution

        # Scale bucket values to match total_count
        scale_factor = total_count / 1000
        bucket_values = [int(v * scale_factor) for v in bucket_values]
        histogram_sum = 0.0

        # Generate bucket metrics
        for i, (le, count) in enumerate(zip(buckets, bucket_values)):
            # Add some variation to counts while maintaining monotonic increase
            if i > 0:
                count = max(count, cumulative_counts[-1])
            cumulative_counts.append(count)

            # Calculate contribution to sum (use middle of bucket for approximation)
            if i > 0:
                prev_count = cumulative_counts[i - 1]
                bucket_count = count - prev_count
                lower_bound = buckets[i - 1]
                upper_bound = le if le != float("inf") else buckets[i - 1] * 2
                avg_value = (lower_bound + upper_bound) / 2
                histogram_sum += avg_value * bucket_count

            metrics.append(
                {
                    "timeseries": [
                        {
                            "labels": [
                                {"name": "__name__", "value": "test_histogram_bucket"},
                                {"name": "instance", "value": "histogram_instance"},
                                {"name": "job", "value": "test_job"},
                                {
                                    "name": "le",
                                    "value": str(le if le != float("inf") else "+Inf"),
                                },
                            ],
                            "samples": [
                                {
                                    "timestamp": timestamp,
                                    "value": float(count),
                                }
                            ],
                        }
                    ]
                }
            )

        # Add histogram sum
        metrics.append(
            {
                "timeseries": [
                    {
                        "labels": [
                            {"name": "__name__", "value": "test_histogram_sum"},
                            {"name": "instance", "value": "histogram_instance"},
                            {"name": "job", "value": "test_job"},
                        ],
                        "samples": [
                            {
                                "timestamp": timestamp,
                                "value": float(histogram_sum),
                            }
                        ],
                    }
                ]
            }
        )

        # Add histogram count (total observations)
        metrics.append(
            {
                "timeseries": [
                    {
                        "labels": [
                            {"name": "__name__", "value": "test_histogram_count"},
                            {"name": "instance", "value": "histogram_instance"},
                            {"name": "job", "value": "test_job"},
                        ],
                        "samples": [
                            {
                                "timestamp": timestamp,
                                "value": float(total_count),
                            }
                        ],
                    }
                ]
            }
        )

        # High cardinality metrics
        for i in range(100):
            base_value = float(i)
            # Add some time-based variation
            time_adjusted_value = base_value + (sample_idx * 0.5)

            metrics.append(
                {
                    "timeseries": [
                        {
                            "labels": [
                                {"name": "__name__", "value": "test_high_cardinality"},
                                {
                                    "name": "instance",
                                    "value": f"high_card_instance_{i}",
                                },
                                {"name": "job", "value": "test_job"},
                                {"name": "id", "value": str(i)},
                            ],
                            "samples": [
                                {"timestamp": timestamp, "value": time_adjusted_value}
                            ],
                        }
                    ]
                }
            )

    return metrics


def write_metrics_to_pushgateway(url: str, metrics: List[Dict]) -> None:
    """Write metrics to Pushgateway in the correct format."""
    # Clear all existing metrics first
    try:
        response = requests.delete(f"{url}/metrics")
        if response.status_code != 204:
            raise Exception(f"Failed to clear metrics: {response.text}")
        time.sleep(1)  # Give it a moment to clear
    except Exception as e:
        print(f"Error clearing metrics: {e!s}")

    # Group metrics by timestamp to simulate time series
    metrics_by_timestamp = {}
    for metric in metrics:
        timestamp = metric["timeseries"][0]["samples"][0]["timestamp"]
        if timestamp not in metrics_by_timestamp:
            metrics_by_timestamp[timestamp] = []
        metrics_by_timestamp[timestamp].append(metric)

    # Process each timestamp group in chronological order (oldest first)
    for timestamp in sorted(metrics_by_timestamp.keys()):
        current_metrics = metrics_by_timestamp[timestamp]

        # Write type declarations for each timestamp
        type_declarations = [
            "# HELP test_counter Test counter metric\n",
            "# TYPE test_counter counter\n",
            "# HELP test_gauge Test gauge metric\n",
            "# TYPE test_gauge gauge\n",
            "# HELP test_histogram_bucket Test histogram metric\n",
            "# TYPE test_histogram_bucket histogram\n",
            "# HELP test_histogram_sum Test histogram sum\n",
            "# TYPE test_histogram_sum counter\n",
            "# HELP test_histogram_count Test histogram count\n",
            "# TYPE test_histogram_count counter\n",
            "# HELP test_high_cardinality Test high cardinality metric\n",
            "# TYPE test_high_cardinality gauge\n",
        ]

        # Write the initial type declarations to its own instance
        response = requests.post(
            f"{url}/metrics/job/test_job/instance/type_declarations",
            data="".join(type_declarations),
            headers={"Content-Type": "text/plain"},
        )
        if response.status_code != 200:
            raise Exception(f"Failed to write type declarations: {response.text}")

        # Group metrics by job and instance
        grouped_metrics = {}
        for metric_data in current_metrics:
            for timeseries in metric_data["timeseries"]:
                labels = {
                    label["name"]: label["value"] for label in timeseries["labels"]
                }
                metric_name = labels.pop("__name__", "unknown_metric")
                instance = labels.pop("instance", "default")
                job = labels.pop("job", "test_job")

                key = (job, instance)
                if key not in grouped_metrics:
                    grouped_metrics[key] = []

                # Add type declaration and help text for each metric group
                if metric_name == "test_counter":
                    metric_help = "# HELP test_counter Test counter metric\n"
                    metric_type = "# TYPE test_counter counter\n"
                elif metric_name == "test_gauge":
                    metric_help = "# HELP test_gauge Test gauge metric\n"
                    metric_type = "# TYPE test_gauge gauge\n"
                elif "test_histogram" in metric_name:
                    metric_help = f"# HELP {metric_name} Test histogram metric\n"
                    metric_type = f"# TYPE {metric_name} {('histogram' if metric_name.endswith('bucket') else 'counter')}\n"
                elif metric_name == "test_high_cardinality":
                    metric_help = (
                        "# HELP test_high_cardinality Test high cardinality metric\n"
                    )
                    metric_type = "# TYPE test_high_cardinality gauge\n"

                if key in grouped_metrics and not any(
                    l.startswith(f"# TYPE {metric_name}") for l in grouped_metrics[key]
                ):
                    grouped_metrics[key].extend([metric_help, metric_type])

                # Format the metric line
                label_pairs = []
                for k, v in sorted(labels.items()):
                    escaped_value = str(v).replace('"', '\\"')
                    label_pairs.append(f'{k}="{escaped_value}"')
                label_str = "{" + ",".join(label_pairs) + "}" if labels else ""

                value = timeseries["samples"][-1]["value"]
                metric_line = f"{metric_name}{label_str} {value}\n"
                grouped_metrics[key].append(metric_line)

        # Write metrics for each job/instance combination
        for (job, instance), metrics_lines in grouped_metrics.items():
            if instance == "type_declarations":
                continue

            response = requests.post(
                f"{url}/metrics/job/{job}/instance/{instance}",
                data="".join(metrics_lines),
                headers={"Content-Type": "text/plain"},
            )
            if response.status_code != 200:
                raise Exception(f"Failed to write metrics: {response.text}")

            time.sleep(0.03)  # Small delay between writes

        time.sleep(0.5)  # Wait between timestamp batches to ensure proper ordering

    # Wait for final scrape
    time.sleep(2)


def verify_data_present(client):
    """Verify test data is present and matches expected values."""
    result = client.query("test_counter")
    if not result.metrics:
        return False

    # Find the highest counter value (should be 90.0)
    max_value = max(metric.latest_value for metric in result.metrics)
    expected_value = EXPECTED_VALUES["test_counter"][0][1]  # Latest value (90.0)
    return (
        abs(max_value - expected_value) < 0.1
    )  # Allow small floating point difference
