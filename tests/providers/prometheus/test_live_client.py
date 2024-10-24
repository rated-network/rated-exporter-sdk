import pytest
from datetime import datetime, timedelta

from rated_exporter_sdk.providers.prometheus.auth import PrometheusAuth
from rated_exporter_sdk.providers.prometheus.client import PrometheusClient
from rated_exporter_sdk.providers.prometheus.errors import (
    PrometheusQueryError,
    PrometheusAPIError,
    PrometheusConnectionError,
    PrometheusTimeoutError,
)
from rated_exporter_sdk.providers.prometheus.types import (
    PrometheusQueryOptions,
    Step,
    TimeUnit,
    MetricValueType,
    TargetHealth,
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

        # Test histogram buckets
        result = client.query("test_histogram_bucket")
        assert result.metrics
        buckets = {
            float(m.identifier.labels["le"]): m.latest_value for m in result.metrics
        }
        assert len(buckets) > 0
        assert all(v2 >= v1 for v1, v2 in zip(buckets.values(), buckets.values()[1:]))

        # Test histogram quantiles
        result = client.query(
            "histogram_quantile(0.95, rate(test_histogram_bucket[5m]))"
        )
        assert result.metrics
        assert len(result.metrics) == 1

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

        # Test timeout
        with pytest.raises(PrometheusTimeoutError):
            client = PrometheusClient(prometheus_with_data["url"], timeout=0.001)
            client.query("sum(rate(test_counter[5m]))")

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
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=2)

        options = PrometheusQueryOptions(
            start_time=start_time,
            end_time=end_time,
            step=Step(value=15, unit=TimeUnit.SECONDS),
        )

        # Stream data in 15-minute chunks
        chunks = list(
            client.stream_query_range(
                "test_counter", options, chunk_size=timedelta(minutes=15)
            )
        )

        assert len(chunks) == 8  # Should have 8 15-minute chunks
        for chunk in chunks:
            assert chunk.metrics
            assert chunk.metrics[0].samples

        # Test streaming with aggregation
        chunks = list(
            client.stream_query_range(
                "sum(rate(test_counter[5m]))", options, chunk_size=timedelta(minutes=15)
            )
        )
        assert len(chunks) == 8
        assert all(chunk.metrics for chunk in chunks)

    def test_metadata_queries(self, prometheus_with_data):
        """Test metadata retrieval functionality."""
        client = PrometheusClient(prometheus_with_data["url"])

        # Test getting all metadata
        metadata = client.get_metric_metadata()
        assert len(metadata) > 0

        # Test getting specific metric metadata
        metadata = client.get_metric_metadata("test_counter")
        assert len(metadata) == 1
        assert metadata[0].metric_name == "test_counter"
        assert metadata[0].type == MetricValueType.COUNTER

    def test_target_health(self, prometheus_with_data):
        """Test target health monitoring functionality."""
        client = PrometheusClient(prometheus_with_data["url"])

        # Get all targets
        targets = client.get_targets()
        assert len(targets) > 0

        # Check specific target health
        up_targets = client.get_targets(state=TargetHealth.UP)
        assert len(up_targets) > 0
        assert all(target.health == TargetHealth.UP for target in up_targets)

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
        assert len(results) == 4
        assert results[0] is not None  # test_counter
        assert results[1] is not None  # test_gauge
        assert results[2] is not None  # rate
        assert results[3] is None  # non-existent

    def test_client_lifecycle(self, prometheus_with_data):
        """Test client lifecycle management."""
        # Test context manager
        with PrometheusClient(prometheus_with_data["url"]) as client:
            result = client.query("test_counter")
            assert result.metrics

        # Test explicit cleanup
        client = PrometheusClient(prometheus_with_data["url"])
        result = client.query("test_counter")
        assert result.metrics
        client.close()

        # Verify session is closed
        with pytest.raises(Exception):
            client.query("test_counter")
