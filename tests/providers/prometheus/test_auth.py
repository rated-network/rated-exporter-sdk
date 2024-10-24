import pytest
import requests
from rated_exporter_sdk.providers.prometheus.auth import PrometheusAuth
from rated_exporter_sdk.providers.prometheus.client import PrometheusClient
from rated_exporter_sdk.providers.prometheus.errors import PrometheusAPIError
from providers.prometheus.helpers.fake_data_generator import (
    generate_test_metrics,
    write_metrics_to_pushgateway,
)


def get_client_with_auth(
    url: str, username: str = None, password: str = None
) -> PrometheusClient:
    """Helper function to create a client with optional auth."""
    if username and password:
        auth = PrometheusAuth(username=username, password=password)
        return PrometheusClient(url, auth=auth)
    return PrometheusClient(url)


class TestPrometheusAuth:
    """Test suite for Prometheus authentication scenarios."""

    def test_no_auth_required(self, prometheus_environment):
        """Test successful queries when no authentication is required."""
        env = prometheus_environment(False)  # Create environment without auth
        client = get_client_with_auth(env["url"])

        result = client.query("up")
        assert result is not None
        assert result.metrics is not None

    def test_valid_auth(self, prometheus_environment):
        """Test successful authentication with valid credentials."""
        env = prometheus_environment(True)
        client = get_client_with_auth(
            env["url"], username="valid_user", password="valid_pass"
        )

        result = client.query("up")
        assert result is not None
        assert result.metrics is not None

    def test_invalid_password(self, prometheus_environment):
        """Test authentication failure with invalid password."""
        env = prometheus_environment(True)  # Create environment with auth enabled
        client = get_client_with_auth(
            env["url"], username="valid_user", password="wrong_password"
        )

        with pytest.raises(PrometheusAPIError) as exc_info:
            client.query("up")
        assert exc_info.value.status_code == 401

    def test_invalid_username(self, prometheus_environment):
        """Test authentication failure with invalid username."""
        env = prometheus_environment(True)  # Create environment with auth enabled
        client = get_client_with_auth(
            env["url"], username="nonexistent_user", password="valid_pass"
        )

        with pytest.raises(PrometheusAPIError) as exc_info:
            client.query("up")
        assert exc_info.value.status_code == 401

    def test_missing_credentials(self, prometheus_environment):
        """Test authentication failure with no credentials when auth is required."""
        env = prometheus_environment(True)  # Create environment with auth enabled
        client = get_client_with_auth(env["url"])

        with pytest.raises(PrometheusAPIError) as exc_info:
            client.query("up")
        assert exc_info.value.status_code == 401

    def test_auth_persistence(self, prometheus_environment):
        """Test that authentication persists across multiple requests."""
        env = prometheus_environment(True)  # Create environment with auth enabled
        client = get_client_with_auth(
            env["url"], username="valid_user", password="valid_pass"
        )

        for _ in range(3):
            result = client.query("up")
            assert result is not None
            assert result.metrics is not None

    @pytest.mark.parametrize(
        "auth_data",
        [
            ("valid_user", "valid_pass", True),
            ("invalid_user", "invalid_pass", False),
            (None, None, False),
        ],
    )
    def test_auth_scenarios(self, prometheus_environment, auth_data):
        """Test various authentication scenarios."""
        username, password, should_succeed = auth_data
        env = prometheus_environment(True)  # Create environment with auth enabled
        client = get_client_with_auth(env["url"], username, password)

        if should_succeed:
            result = client.query("up")
            assert result is not None
            assert result.metrics is not None
        else:
            with pytest.raises(PrometheusAPIError) as exc_info:
                client.query("up")
            assert exc_info.value.status_code == 401
