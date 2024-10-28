import pytest
from rated_exporter_sdk.providers.prometheus.client import (
    PrometheusClient,
    QueryValidator,
)
from rated_exporter_sdk.providers.prometheus.errors import (
    PrometheusConnectionError,
    PrometheusQueryError,
)


@pytest.mark.provider("prometheus")
class TestMockPrometheusClient:
    """Test suite for PrometheusClient."""

    def test_invalid_query(self, prometheus_environment):
        """Test handling of invalid queries."""
        env = prometheus_environment(False)
        client = PrometheusClient(env["url"])

        with pytest.raises(PrometheusQueryError):
            client.query("invalid{metric")

    def test_error_handling(self, prometheus_environment):
        """Test various error conditions."""
        # Test connection error
        with pytest.raises(PrometheusConnectionError):
            client = PrometheusClient("http://nonexistent:9090")
            client.query("up")

        # Test timeout
        env = prometheus_environment(False)
        client = PrometheusClient(env["url"], timeout=0.001)  # Very short timeout
        with pytest.raises(PrometheusConnectionError):
            client.query("sum(rate(test_counter[5m]))")

    @pytest.mark.parametrize(
        "query, valid",
        [
            # Valid queries
            ("metric_name", True),
            ('metric_name{label="value"}', True),
            ("rate(metric_name[5m])", True),
            ("sum(rate(metric_name[5m]))", True),
            ('metric_name{label=~"value.*"}', True),
            ('metric_name{label!~"value.*",label2!="other"}', True),
            ("metric_name[5m]", True),
            ('metric_name{label="value"}[5m]', True),
            ('sum by (label) (metric_name{label="value"})', True),
            ("avg_over_time(metric_name[5m])", True),
            ("histogram_quantile(0.95, rate(metric_name_bucket[5m]))", True),
            ('{label="value"}', True),  # Missing metric name is allowed
            ("metric_name[5m] offset 5m", True),
            # Invalid queries
            ("invalid{metric", False),
            ("metric_name{label=value}", False),  # Missing quotes
            ("rate(metric_name[5m)", False),
            ('metric_name{label="}', False),  # Incomplete label value
            ("metric_name{=value}", False),  # Missing label name
            ('metric_name{label=="value"}', False),  # Invalid operator '=='
            ('metric_name{label="value}', False),  # Missing closing quote
            ("metric_name{", False),  # Missing closing '}'
            ('metric_name{label="value",}', False),  # Trailing comma
            ("metric_name{label}", False),  # Missing operator and value
            ('metric_name{label="value"', False),  # Missing closing '}'
            ("sum(", False),  # Missing closing ')'
            ("metric_name[", False),  # Missing closing ']'
            ("metric_name[5m", False),  # Missing closing ']'
            ("metric_name[5m]offset", False),  # Missing offset duration
            ("metric_name offset 5m", False),  # Wrong syntax
        ],
    )
    def test_query_validation(self, query, valid):
        """Test PromQL query validation."""
        validator = QueryValidator()

        if valid:
            validator.validate_query(query)
        else:
            with pytest.raises(PrometheusQueryError):
                validator.validate_query(query)
