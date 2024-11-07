import base64
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from rated_exporter_sdk.core.exceptions import AuthenticationError, ConfigurationError


@dataclass
class PrometheusAuth:
    """
    Authentication configuration for Prometheus.
    Supports basic auth, token auth, and SSL/TLS configuration.
    """

    # Basic auth credentials
    username: Optional[str] = None
    password: Optional[str] = None

    # Token auth
    token: Optional[str] = None
    token_file: Optional[Union[str, Path]] = None

    # SSL/TLS configuration
    ssl_verify: bool = True
    ssl_cert: Optional[Union[str, Path]] = None
    ssl_key: Optional[Union[str, Path]] = None
    ca_cert: Optional[Union[str, Path]] = None

    def __post_init__(self):
        """Validate authentication configuration after initialization."""
        # Convert string paths to Path objects
        if isinstance(self.token_file, str):
            self.token_file = Path(self.token_file)
        if isinstance(self.ssl_cert, str):
            self.ssl_cert = Path(self.ssl_cert)
        if isinstance(self.ssl_key, str):
            self.ssl_key = Path(self.ssl_key)
        if isinstance(self.ca_cert, str):
            self.ca_cert = Path(self.ca_cert)

        self._validate_auth_config()
        self._validate_ssl_config()

    def _validate_auth_config(self):
        """Validate authentication configuration."""
        auth_methods = [
            bool(self.token),
            bool(self.token_file),
            bool(self.username and self.password),
        ]

        if sum(auth_methods) > 1:
            raise ConfigurationError(
                "Multiple authentication methods specified. "
                "Use either token, token_file, or username/password."
            )

        if bool(self.username) != bool(self.password):
            raise ConfigurationError(
                "Both username and password must be provided for basic authentication"
            )

        if self.token_file and not self.token_file.is_file():
            raise ConfigurationError(f"Token file not found: {self.token_file}")

    def _validate_ssl_config(self):
        """Validate SSL/TLS configuration."""
        if self.ssl_cert and not self.ssl_key:
            raise ConfigurationError(
                "SSL key must be provided when using SSL certificate"
            )

        if self.ssl_key and not self.ssl_cert:
            raise ConfigurationError(
                "SSL certificate must be provided when using SSL key"
            )

        if self.ssl_cert and not self.ssl_cert.is_file():
            raise ConfigurationError(f"SSL certificate not found: {self.ssl_cert}")

        if self.ssl_key and not self.ssl_key.is_file():
            raise ConfigurationError(f"SSL key not found: {self.ssl_key}")

        if self.ca_cert and not self.ca_cert.is_file():
            raise ConfigurationError(f"CA certificate not found: {self.ca_cert}")

    def get_auth_headers(self) -> Dict[str, Any]:
        """
        Generate authentication headers based on configuration.

        Returns:
            dict: Headers to be included in requests

        Raises:
            AuthenticationError: If token file cannot be read
        """
        headers: Dict[str, Union[bool, Tuple[str, str], str]] = {
            "Content-Type": "application/json"
        }

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        elif isinstance(self.token_file, Path):
            try:
                token: str = self.token_file.read_text().strip()
                headers["Authorization"] = f"Bearer {token}"
            except (IOError, OSError) as e:
                raise AuthenticationError(f"Failed to read token file: {e!s}") from e
        elif self.username and self.password:
            auth_string = base64.b64encode(
                f"{self.username}:{self.password}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {auth_string}"
        return headers

    def get_ssl_config(self) -> Dict[str, Union[bool, Tuple[str, str], str]]:
        """
        Get SSL configuration for requests.

        Returns:
            dict: SSL configuration parameters
        """
        ssl_config: Dict[str, Union[bool, Tuple[str, str], str]] = {
            "verify": self.ssl_verify
        }

        if self.ca_cert:
            ssl_config["verify"] = str(self.ca_cert)

        if self.ssl_cert and self.ssl_key:
            ssl_config["cert"] = (str(self.ssl_cert), str(self.ssl_key))

        return ssl_config

    @classmethod
    def from_env(cls) -> "PrometheusAuth":
        """
        Create authentication configuration from environment variables.

        Environment variables:
            PROMETHEUS_USERNAME: Basic auth username
            PROMETHEUS_PASSWORD: Basic auth password
            PROMETHEUS_TOKEN: Bearer token
            PROMETHEUS_TOKEN_FILE: Path to token file
            PROMETHEUS_SSL_VERIFY: Whether to verify SSL (true/false)
            PROMETHEUS_SSL_CERT: Path to SSL certificate
            PROMETHEUS_SSL_KEY: Path to SSL key
            PROMETHEUS_CA_CERT: Path to CA certificate

        Returns:
            PrometheusAuth: Authentication configuration
        """
        return cls(
            username=os.getenv("PROMETHEUS_USERNAME"),
            password=os.getenv("PROMETHEUS_PASSWORD"),
            token=os.getenv("PROMETHEUS_TOKEN"),
            token_file=os.getenv("PROMETHEUS_TOKEN_FILE"),
            ssl_verify=os.getenv("PROMETHEUS_SSL_VERIFY", "true").lower() == "true",
            ssl_cert=os.getenv("PROMETHEUS_SSL_CERT"),
            ssl_key=os.getenv("PROMETHEUS_SSL_KEY"),
            ca_cert=os.getenv("PROMETHEUS_CA_CERT"),
        )
