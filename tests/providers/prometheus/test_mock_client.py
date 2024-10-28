import pytest
from rated_exporter_sdk.providers.prometheus.client import PrometheusClient
from rated_exporter_sdk.providers.prometheus.errors import (
    PrometheusConnectionError,
    PrometheusQueryError,
)


@pytest.mark.provider("prometheus")
class TestMockPrometheusClient:
    """Test suite for PrometheusClient."""

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
