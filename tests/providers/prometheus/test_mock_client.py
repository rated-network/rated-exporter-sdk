import time

import pytest
from datetime import datetime, timedelta


from providers.prometheus.helpers.fake_data_generator import (
    EXPECTED_VALUES,
    verify_data_present,
    write_metrics_to_pushgateway,
    generate_test_metrics,
)
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
class TestMockPrometheusClient:
    """Test suite for PrometheusClient."""

    def test_basic_query(self, prometheus_container):
        """Test basic instant query functionality with verified data."""
        client = PrometheusClient(prometheus_container["url"])

        # Write and verify test data
        write_metrics_to_pushgateway(
            prometheus_container["pushgateway_url"], generate_test_metrics()
        )

        # Wait for data to be available
        max_retries = 10
        for _ in range(max_retries):
            if verify_data_present(client):
                break
            time.sleep(1)
        else:
            pytest.fail("Test data not available after waiting")

        # Now test the query
        result = client.query("test_counter")
        assert result.metrics
        assert result.metrics[0].identifier.name == "test_counter"
        assert (
            abs(result.metrics[0].latest_value - EXPECTED_VALUES["test_counter"][0][1])
            < 0.1
        )

    def test_range_query(self, prometheus_container):
        """Test range query functionality with verified data."""
        client = PrometheusClient(prometheus_container["url"])

        # Verify data is present
        if not verify_data_present(client):
            pytest.fail("Test data not available")

        # Use fixed timestamps for consistent results
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=10)

        options = PrometheusQueryOptions(
            start_time=start_time,
            end_time=end_time,
            step=Step(value=60, unit=TimeUnit.SECONDS),
        )

        result = client.query_range("test_counter", options)

        assert result.metrics
        assert len(result.metrics[0].samples) > 0
        assert result.metrics[0].identifier.name == "test_counter"

        # Verify we have the expected range of values
        values = [sample.value for sample in result.metrics[0].samples]
        assert min(values) >= 0.0  # Minimum value from test data
        assert max(values) <= 90.0  # Maximum value from test data

    def test_batch_queries(self, prometheus_container):
        """Test batch queries with exact value verification."""
        client = PrometheusClient(prometheus_container["url"])

        # Verify data is present
        if not verify_data_present(client):
            pytest.fail("Test data not available")

        queries = ["test_counter", "test_gauge", "non_existent_metric"]
        results = client.query_batch(queries)

        assert len(results) == 3

        # Verify test_counter
        assert results[0].metrics
        assert results[0].metrics[0].identifier.name == "test_counter"
        assert (
            abs(
                results[0].metrics[0].latest_value
                - EXPECTED_VALUES["test_counter"][0][1]
            )
            < 0.1
        )

        # Verify test_gauge
        assert results[1].metrics
        assert results[1].metrics[0].identifier.name == "test_gauge"
        assert (
            abs(
                results[1].metrics[0].latest_value - EXPECTED_VALUES["test_gauge"][0][1]
            )
            < 0.1
        )

        # Verify non_existent_metric
        assert not results[2].metrics

    def test_streaming_large_dataset(self, prometheus_container):
        """Test streaming large dataset with exact chunk verification."""
        client = PrometheusClient(prometheus_container["url"])

        # Verify data is present
        if not verify_data_present(client):
            pytest.fail("Test data not available")

        # Use exact timestamps for consistent chunking
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        chunk_size = timedelta(minutes=15)

        options = PrometheusQueryOptions(
            start_time=start_time,
            end_time=end_time,
            step=Step(value=15, unit=TimeUnit.SECONDS),
        )

        chunks = list(
            client.stream_query_range("test_counter", options, chunk_size=chunk_size)
        )

        # Calculate exact number of chunks
        expected_chunks = 4  # 1 hour / 15 minutes = 4 chunks
        assert len(chunks) == expected_chunks

        # Verify each chunk has data
        for chunk in chunks:
            assert chunk.metrics
            assert chunk.metrics[0].identifier.name == "test_counter"
            assert len(chunk.metrics[0].samples) > 0
            # Verify values are within expected range
            for sample in chunk.metrics[0].samples:
                assert 0 <= sample.value <= 90.0

    def test_invalid_query(self, prometheus_container):
        """Test handling of invalid queries."""
        client = PrometheusClient(prometheus_container["url"])

        with pytest.raises(PrometheusQueryError):
            client.query("invalid{metric")

    def test_error_handling(self, prometheus_container):
        """Test various error conditions."""
        # Test connection error
        with pytest.raises(PrometheusConnectionError):
            client = PrometheusClient("http://nonexistent:9090")
            client.query("up")

        # Test timeout
        client = PrometheusClient(
            prometheus_container["url"], timeout=0.001  # Very short timeout
        )
        with pytest.raises(PrometheusConnectionError):
            client.query("sum(rate(test_counter[5m]))")

    @pytest.mark.parametrize(
        "query,valid",
        [
            ("metric_name", True),
            ('metric_name{label="value"}', True),
            ("rate(metric_name[5m])", True),
            ("sum(rate(metric_name[5m]))", True),
            ("invalid{metric", False),
            ("metric_name{label=value}", False),  # Missing quotes
            ("rate(metric_name[5m)", False),
            ('metric_name{label="}', False),  # Incomplete label value
            ("metric_name{=value}", False),  # Missing label name
            ('{label="value"}', False),  # Missing metric name
        ],
    )
    def test_query_validation(self, query, valid):
        """Test PromQL query validation."""
        client = PrometheusClient("http://localhost:9090")

        if valid:
            client.validate_query(query)
        else:
            with pytest.raises(PrometheusQueryError):
                client.validate_query(query)
