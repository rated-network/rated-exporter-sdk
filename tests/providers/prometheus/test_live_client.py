import time
from datetime import datetime, timedelta

import pytest
from rated_exporter_sdk.providers.prometheus.client import PrometheusClient
from rated_exporter_sdk.providers.prometheus.errors import (
    PrometheusConnectionError,
    PrometheusQueryError,
)
from rated_exporter_sdk.providers.prometheus.types import (
    MetricValueType,
    PrometheusQueryOptions,
    Step,
    TargetHealth,
    TimeUnit,
)


@pytest.mark.provider("prometheus")
class TestLivePrometheusClient:
    """Comprehensive test suite for PrometheusClient with live data."""

    def test_instant_queries(self, prometheus_with_data):
        """Test various instant query scenarios."""
        client = PrometheusClient(prometheus_with_data["url"])

        # Test counter query
        result = client.query("test_counter")
        assert result.metrics
        assert result.metrics[0].identifier.name == "test_counter"
        assert result.metrics[0].type == MetricValueType.COUNTER
        assert result.metrics[0].latest_value > 0

        # Test gauge query
        result = client.query("test_gauge")
        assert result.metrics
        assert result.metrics[0].identifier.name == "test_gauge"
        assert result.metrics[0].type == MetricValueType.GAUGE

        # Test complex query with functions
        result = client.query("rate(test_counter[5m])")
        assert result.metrics
        assert len(result.metrics) > 0

        # Test query with labels
        result = client.query('test_counter{job="test_job"}')
        assert result.metrics
        assert result.metrics[0].identifier.labels["job"] == "test_job"

    def test_range_queries(self, prometheus_with_data):
        """Test range query functionality with various scenarios."""
        client = PrometheusClient(prometheus_with_data["url"])
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)

        options = PrometheusQueryOptions(
            start_time=start_time,
            end_time=end_time,
            step=Step(value=15, unit=TimeUnit.SECONDS),
        )

        # Test basic range query
        result = client.query_range("test_counter", options)
        assert result.metrics
        assert len(result.metrics[0].samples) > 1

        # Test rate function over range
        result = client.query_range("rate(test_counter[5m])", options)
        assert result.metrics
        samples = result.metrics[0].samples
        assert len(samples) > 1
        assert all(sample.value >= 0 for sample in samples)  # Rates should be positive

        # Test aggregate functions
        result = client.query_range("sum(test_high_cardinality)", options)
        assert result.metrics
        assert len(result.metrics) == 1  # Should be aggregated to single series

    def test_histogram_queries(self, prometheus_with_data):
        """Test histogram-specific functionality."""
        client = PrometheusClient(prometheus_with_data["url"])

        result = client.query('test_histogram_bucket_bucket{job="test_job"}')
        assert result.metrics, "No histogram metrics found after retries"

        buckets = {
            float(m.identifier.labels["le"]): m.latest_value
            for m in result.metrics
            if "le" in m.identifier.labels
        }
        assert len(buckets) > 0, "No histogram buckets found"

        sorted_values = [v for _, v in sorted(buckets.items())]
        assert all(
            v2 >= v1 for v1, v2 in zip(sorted_values, sorted_values[1:])
        ), f"Bucket values not monotonically increasing: {sorted_values}"

        result = client.query(
            'histogram_quantile(0.95, sum by (le) (test_histogram_bucket_bucket{job="test_job"}))'
        )
        assert result.metrics, "No quantile metrics found"
        assert len(result.metrics) == 1, "Expected exactly one quantile result"

    def test_high_cardinality_handling(self, prometheus_with_data):
        """Test handling of high cardinality metrics."""
        client = PrometheusClient(prometheus_with_data["url"])

        # Test querying all series
        result = client.query("test_high_cardinality")
        assert len(result.metrics) == 100  # Should get all instances

        # Test with limiting
        result = client.query("topk(10, test_high_cardinality)")
        assert len(result.metrics) <= 10

    def test_error_handling(self, prometheus_with_data):
        """Test various error scenarios."""
        client = PrometheusClient(prometheus_with_data["url"])

        # Test syntax error
        with pytest.raises(PrometheusQueryError):
            client.query("invalid{metric")

        # Test invalid metric name
        with pytest.raises(PrometheusQueryError):
            client.validate_query("1invalid_metric")

        # Test connection error
        with pytest.raises(PrometheusConnectionError):
            client = PrometheusClient("http://nonexistent:9090")
            client.query("up")

    def test_streaming_functionality(self, prometheus_with_data):
        """Test streaming of large datasets."""
        client = PrometheusClient(prometheus_with_data["url"])

        # First verify what data is actually available
        print("\nChecking available counter data...")
        result = client.query('test_counter{job="test_job"}')
        if result.metrics:
            print(f"Found {len(result.metrics)} counter metrics")
            for metric in result.metrics:
                print(
                    f"Instance: {metric.identifier.labels.get('instance')}, Value: {metric.latest_value}"
                )
        else:
            print("No counter metrics found in initial check")

        # Use a shorter time range and ensure it starts from a point with data
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=2)  # Very short range to debug

        options = PrometheusQueryOptions(
            start_time=start_time,
            end_time=end_time,
            step=Step(value=15, unit=TimeUnit.SECONDS),
        )

        # Check full range query first
        print("\nChecking range query...")
        test_result = client.query_range('test_counter{job="test_job"}', options)
        if test_result.metrics:
            print(f"Found {len(test_result.metrics)} metrics in range query")
            for metric in test_result.metrics:
                print(f"Instance: {metric.identifier.labels.get('instance')}")
                print(f"Sample count: {len(metric.samples)}")
                if metric.samples:
                    print(
                        f"Time range: {metric.samples[0].timestamp} to {metric.samples[-1].timestamp}"
                    )
        else:
            print("No data found in range query")

        assert test_result.metrics, "No data available in the time range"

        # Test streaming with very small chunks for debugging
        print("\nTesting streaming...")
        chunk_size = timedelta(seconds=30)  # Reduced for debugging
        chunks = list(
            client.stream_query_range(
                'test_counter{job="test_job"}', options, chunk_size=chunk_size
            )
        )

        # Print chunk information
        print(f"\nReceived {len(chunks)} chunks")
        for i, chunk in enumerate(chunks):
            print(f"\nChunk {i}:")
            if chunk.metrics:
                print(f"Contains {len(chunk.metrics)} metrics")
                for metric in chunk.metrics:
                    print(f"Instance: {metric.identifier.labels.get('instance')}")
                    print(f"Sample count: {len(metric.samples)}")
                    if metric.samples:
                        print(
                            f"Time range: {metric.samples[0].timestamp} to {metric.samples[-1].timestamp}"
                        )
            else:
                print("No metrics in chunk")

        assert any(chunk.metrics for chunk in chunks), "No data found in any chunks"

        # Collect and verify all samples
        all_samples = []
        for chunk in chunks:
            for metric in chunk.metrics:
                # Add instance label to help debug timestamp ordering
                instance = metric.identifier.labels.get("instance", "unknown")
                all_samples.extend(
                    (sample.timestamp, instance, sample.value)
                    for sample in metric.samples
                )

        if all_samples:
            # Sort by timestamp and print for debugging
            all_samples.sort(key=lambda x: x[0])
            print("\nSample timestamps in order:")
            for timestamp, instance, value in all_samples:
                print(f"Time: {timestamp}, Instance: {instance}, Value: {value}")

            # Now verify ordering
            timestamps = [x[0] for x in all_samples]
            assert all(
                timestamps[i] <= timestamps[i + 1] for i in range(len(timestamps) - 1)
            ), "Samples are not in chronological order"

    def test_metadata_queries(self, prometheus_with_data):
        """Test metadata retrieval functionality."""
        client = PrometheusClient(prometheus_with_data["url"])

        metadata = client.get_metric_metadata("test_counter")
        print(metadata)
        assert len(metadata) >= 1
        counter_metadata = next(
            (m for m in metadata if m.metric_name == "test_counter"), None
        )
        assert counter_metadata is not None
        assert counter_metadata.type == MetricValueType.COUNTER
        assert counter_metadata.help == "Test counter metric"

    @pytest.mark.skip(reason="Requires Prometheus with write access")
    def test_target_health(self, prometheus_with_data):
        """Test target health monitoring functionality."""
        client = PrometheusClient(prometheus_with_data["url"])

        # Wait for targets to be discovered
        time.sleep(2)

        targets = client.get_targets()
        active_targets = [t for t in targets if t.health == TargetHealth.UP]
        assert len(active_targets) > 0, "No active targets found"

        pushgateway_target = next(
            (t for t in active_targets if t.job == "test_job"), None
        )
        assert pushgateway_target is not None, "Pushgateway target not found"
        assert pushgateway_target.health == TargetHealth.UP

    def test_series_queries(self, prometheus_with_data):
        """Test series querying functionality."""
        client = PrometheusClient(prometheus_with_data["url"])
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)

        # Test getting all series for a metric
        series = client.get_series(
            match=["test_counter"], start_time=start_time, end_time=end_time
        )
        assert len(series) > 0
        assert any(s.name == "test_counter" for s in series)

        # Test with label matchers
        series = client.get_series(
            match=['test_counter{job="test_job"}'],
            start_time=start_time,
            end_time=end_time,
        )
        assert len(series) > 0
        assert all(s.labels.get("job") == "test_job" for s in series)

    def test_batch_queries(self, prometheus_with_data):
        """Test batch query functionality."""
        client = PrometheusClient(prometheus_with_data["url"])

        queries = [
            "test_counter",
            "test_gauge",
            "rate(test_counter[5m])",
            "non_existent_metric",
        ]

        results = client.query_batch(queries)
        assert len(results) == 4, "Should return results for all queries"

        # Test counter query should succeed
        assert results[0] is not None, "test_counter query should return data"
        assert results[0].metrics, "test_counter should have metrics"
        assert results[0].metrics[0].identifier.name == "test_counter"

        # Test gauge query should succeed
        assert results[1] is not None, "test_gauge query should return data"
        assert results[1].metrics, "test_gauge should have metrics"
        assert results[1].metrics[0].identifier.name == "test_gauge"

        # Rate query should succeed
        assert results[2] is not None, "rate query should return data"
        assert results[2].metrics, "rate query should have metrics"

        # Non-existent metric should return None
        assert results[3] is None, "Query for non-existent metric should return None"

        # Test batch query with empty list
        empty_results = client.query_batch([])
        assert empty_results == [], "Empty query list should return empty result list"

        # Test batch query with invalid queries
        invalid_queries = ["invalid{metric", "test_counter{invalid=]", "sum("]
        invalid_results = client.query_batch(invalid_queries)
        assert all(
            result is None for result in invalid_results
        ), "Invalid queries should return None"
