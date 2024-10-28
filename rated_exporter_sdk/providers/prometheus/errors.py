from typing import Any, Dict, Optional

from rated_exporter_sdk.core.exceptions import (
    APIError,
    ConfigurationError,
    DataSourceError,
    QueryError,
)


class PrometheusAPIError(APIError):
    """
    Raised when Prometheus API returns an error.

    Attributes:
        message: Error message
        status_code: HTTP status code
        response: Raw API response
        query: Original query that caused the error
    """

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response: Optional[Dict[str, Any]] = None,
        query: Optional[str] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response = response
        self.query = query

    def __str__(self) -> str:
        parts = [self.args[0]]
        if self.status_code:
            parts.append(f"Status code: {self.status_code}")
        if self.query:
            parts.append(f"Query: {self.query}")
        return " | ".join(parts)


class PrometheusQueryError(QueryError):
    """
    Raised when there's an error in PromQL query syntax or execution.

    Attributes:
        message: Error message
        query: The invalid query
        position: Position in query where error occurred
    """

    def __init__(
        self, message: str, query: Optional[str] = None, position: Optional[int] = None
    ):
        super().__init__(message)
        self.query = query
        self.position = position

    def __str__(self) -> str:
        parts = [self.args[0]]
        if self.query:
            parts.append(f"Query: {self.query}")
            if self.position is not None:
                parts.append(f"Position: {self.position}")
                # Add visual indicator of error position
                parts.append("\n" + self.query)
                parts.append(" " * self.position + "^")
        return " | ".join(parts)


class PrometheusConnectionError(DataSourceError):
    """
    Raised when there's an error connecting to Prometheus.

    Attributes:
        message: Error message
        url: The URL that failed
        original_error: The original exception
    """

    def __init__(
        self,
        message: str,
        url: Optional[str] = None,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.url = url
        self.original_error = original_error

    def __str__(self) -> str:
        parts = [self.args[0]]
        if self.url:
            parts.append(f"URL: {self.url}")
        if self.original_error:
            parts.append(f"Original error: {self.original_error!s}")
        return " | ".join(parts)


class PrometheusTimeoutError(APIError):
    """
    Raised when a Prometheus query times out.

    Attributes:
        message: Error message
        timeout: The timeout value that was exceeded
        query: The query that timed out
    """

    def __init__(self, timeout: float, query: Optional[str] = None):
        message = f"Query timed out after {timeout} seconds"
        if query:
            message += f" | Query: {query}"
        super().__init__(message)
        self.timeout = timeout
        self.query = query


class PrometheusValidationError(ConfigurationError):
    """
    Raised when there's a validation error in Prometheus configuration.

    Attributes:
        message: Error message
        parameter: The invalid parameter
        value: The invalid value
    """

    def __init__(
        self, message: str, parameter: Optional[str] = None, value: Optional[Any] = None
    ):
        super().__init__(message)
        self.parameter = parameter
        self.value = value

    def __str__(self) -> str:
        parts = [self.args[0]]
        if self.parameter:
            parts.append(f"Parameter: {self.parameter}")
            if self.value is not None:
                parts.append(f"Value: {self.value}")
        return " | ".join(parts)
