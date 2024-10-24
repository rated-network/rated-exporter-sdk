from typing import Optional, Dict, Any, List, Union, Generator
from datetime import datetime, timedelta
import requests
from urllib.parse import urljoin, quote
import logging
from requests.adapters import HTTPAdapter, Retry
from concurrent.futures import ThreadPoolExecutor
import re

from rated_exporter_sdk.providers.prometheus.auth import PrometheusAuth
from rated_exporter_sdk.providers.prometheus.types import (
    PrometheusMetric,
    PrometheusQueryResult,
    PrometheusQueryOptions,
    MetricType,
    MetricValueType,
    MetricIdentifier,
    Sample,
    Target,
    TargetHealth,
    MetricMetadata,
    Step,
    TimeUnit,
    TimeRange,
)
from rated_exporter_sdk.providers.prometheus.errors import (
    PrometheusAPIError,
    PrometheusQueryError,
    PrometheusConnectionError,
    PrometheusTimeoutError,
    PrometheusValidationError,
)

logger = logging.getLogger(__name__)


class PrometheusClient:
    """
    Client for interacting with Prometheus API.

    Features:
    - Robust error handling and retries
    - Query validation
    - Batch querying capabilities
    - Streaming support for large datasets
    - Automatic rate limiting
    - Connection pooling
    """

    # PromQL query validation patterns
    METRIC_NAME_PATTERN = re.compile(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$")
    LABEL_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    # API endpoints
    QUERY_ENDPOINT = "/api/v1/query"
    QUERY_RANGE_ENDPOINT = "/api/v1/query_range"
    SERIES_ENDPOINT = "/api/v1/series"
    TARGETS_ENDPOINT = "/api/v1/targets"
    METADATA_ENDPOINT = "/api/v1/metadata"
    RULES_ENDPOINT = "/api/v1/rules"
    ALERTS_ENDPOINT = "/api/v1/alerts"

    def __init__(
        self,
        base_url: str,
        auth: Optional[PrometheusAuth] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_backoff_factor: float = 0.1,
        pool_connections: int = 10,
        pool_maxsize: int = 10,
        max_parallel_queries: int = 5,
    ):
        """
        Initialize Prometheus client.

        Args:
            base_url: Base URL of Prometheus instance
            auth: Authentication configuration
            timeout: Default timeout for requests in seconds
            max_retries: Maximum number of retries for failed requests
            retry_backoff_factor: Backoff factor between retries
            pool_connections: Number of connection pools to maintain
            pool_maxsize: Maximum size of each connection pool
            max_parallel_queries: Maximum number of parallel queries

        Raises:
            PrometheusValidationError: If configuration is invalid
        """
        if not base_url:
            raise PrometheusValidationError(
                "Base URL must be provided", parameter="base_url"
            )

        self.base_url = base_url.rstrip("/")
        self.auth = auth or PrometheusAuth()
        self.timeout = timeout
        self.max_parallel_queries = max_parallel_queries

        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=retry_backoff_factor,
            status_forcelist=[408, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST"],
        )

        # Configure connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
        )

        # Initialize session
        self.session = requests.Session()
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Configure authentication
        self.session.headers.update(self.auth.get_auth_headers())
        ssl_config = self.auth.get_ssl_config()
        self.session.verify = ssl_config["verify"]
        if "cert" in ssl_config:
            self.session.cert = ssl_config["cert"]

        # Initialize thread pool for parallel queries
        self.thread_pool = ThreadPoolExecutor(max_workers=max_parallel_queries)

    def validate_query(self, query: str) -> None:
        """
        Validate PromQL query syntax.

        Args:
            query: PromQL query string

        Raises:
            PrometheusQueryError: If query is invalid
        """
        if not query or not isinstance(query, str):
            raise PrometheusQueryError("Query must be a non-empty string", query=query)

        # Helper function to check bracket matching (excluding quotes)
        def check_brackets(s: str) -> bool:
            stack = []
            brackets = {"(": ")", "{": "}", "[": "]"}
            in_quotes = False

            for i, char in enumerate(s):
                if char == '"':
                    in_quotes = not in_quotes
                    continue

                if in_quotes:
                    continue

                if char in brackets:
                    stack.append(char)
                elif char in brackets.values():
                    if not stack:
                        return False
                    if char != brackets[stack.pop()]:
                        return False

            return len(stack) == 0 and not in_quotes

        # Check for balanced brackets
        if not check_brackets(query):
            raise PrometheusQueryError("Unmatched brackets", query=query)

        # Check if query starts with a bracket (missing metric name)
        if query.lstrip().startswith("{"):
            raise PrometheusQueryError(
                "Missing metric name before label specifications", query=query
            )

        # Parse metric name and labels
        if "{" in query:
            try:
                metric_part = query.split("{")[0].strip()
                label_part = query[query.index("{") : query.index("}") + 1]

                # Explicitly check for empty metric name
                if not metric_part:
                    raise PrometheusQueryError(
                        "Missing metric name before label specifications", query=query
                    )
            except ValueError:
                raise PrometheusQueryError("Invalid label syntax", query=query)

            # Validate metric name
            if not self.METRIC_NAME_PATTERN.match(metric_part):
                raise PrometheusQueryError(
                    f"Invalid metric name: {metric_part}", query=query
                )

            # Validate label syntax
            if label_part != "{}":
                label_content = label_part[1:-1].strip()
                if label_content:  # If there are labels
                    for label_expr in label_content.split(","):
                        label_expr = label_expr.strip()
                        if not label_expr:
                            continue

                        # Check label format
                        if "=" not in label_expr:
                            raise PrometheusQueryError(
                                f"Invalid label format: {label_expr}", query=query
                            )

                        label_name, label_value = label_expr.split("=", 1)
                        label_name = label_name.strip()
                        label_value = label_value.strip()

                        # Validate label name
                        if not self.LABEL_NAME_PATTERN.match(label_name):
                            raise PrometheusQueryError(
                                f"Invalid label name: {label_name}", query=query
                            )

                        # Validate label value quotes
                        if not (
                            label_value.startswith('"') and label_value.endswith('"')
                        ):
                            raise PrometheusQueryError(
                                f"Label value must be quoted: {label_value}",
                                query=query,
                            )

        # Validate duration syntax if present (e.g., [5m])
        duration_pattern = r"\[\d+[smhdwy]\]"
        if "[" in query:
            try:
                duration_part = query[query.index("[") : query.index("]") + 1]
                if not re.match(duration_pattern, duration_part):
                    raise PrometheusQueryError(
                        f"Invalid duration format: {duration_part}", query=query
                    )
            except ValueError:
                raise PrometheusQueryError("Invalid duration syntax", query=query)

    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """
        Make HTTP request to Prometheus API.

        Args:
            method: HTTP method
            endpoint: API endpoint
            **kwargs: Additional request parameters

        Returns:
            Dict containing API response

        Raises:
            PrometheusConnectionError: If connection fails
            PrometheusAPIError: If API returns an error
            PrometheusTimeoutError: If request times out
        """
        url = urljoin(self.base_url, endpoint)
        timeout = kwargs.pop("timeout", self.timeout)

        try:
            response = self.session.request(
                method=method, url=url, timeout=timeout, **kwargs
            )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout as e:
            raise PrometheusTimeoutError(
                timeout, query=kwargs.get("params", {}).get("query")
            ) from e
        except requests.exceptions.ConnectionError as e:
            raise PrometheusConnectionError(
                f"Failed to connect to Prometheus at {self.base_url}",
                url=url,
                original_error=e,
            ) from e
        except requests.exceptions.HTTPError as e:
            raise PrometheusAPIError(
                str(e),
                status_code=e.response.status_code,
                response=e.response.json() if e.response.text else None,
                query=kwargs.get("params", {}).get("query"),
            ) from e
        except Exception as e:
            raise PrometheusAPIError(f"Unexpected error: {str(e)}") from e

    def query(
        self, query: str, options: Optional[PrometheusQueryOptions] = None
    ) -> PrometheusQueryResult:
        """
        Execute an instant query against Prometheus.

        Args:
            query: PromQL query string
            options: Query options

        Returns:
            PrometheusQueryResult containing query results

        Raises:
            PrometheusQueryError: If query is invalid
            PrometheusAPIError: If query execution fails
        """
        self.validate_query(query)

        params = {"query": query}
        if options:
            if options.time:
                params["time"] = options.time.timestamp()
            if options.timeout:
                params["timeout"] = f"{options.timeout}s"

        response = self._make_request("GET", self.QUERY_ENDPOINT, params=params)

        return self._parse_response(response)

    def query_range(
        self, query: str, options: PrometheusQueryOptions
    ) -> PrometheusQueryResult:
        """
        Execute a range query against Prometheus.

        Args:
            query: PromQL query string
            options: Query options (must include start_time, end_time, and step)

        Returns:
            PrometheusQueryResult containing query results

        Raises:
            PrometheusQueryError: If query is invalid
            PrometheusAPIError: If query execution fails
        """
        self.validate_query(query)

        if not (options.start_time and options.end_time and options.step):
            raise PrometheusValidationError(
                "start_time, end_time, and step are required for range queries"
            )

        params = {
            "query": query,
            "start": options.start_time.timestamp(),
            "end": options.end_time.timestamp(),
            "step": str(options.step),
        }

        if options.timeout:
            params["timeout"] = f"{options.timeout}s"

        response = self._make_request("GET", self.QUERY_RANGE_ENDPOINT, params=params)

        return self._parse_response(response)

    def query_batch(
        self, queries: List[str], options: Optional[PrometheusQueryOptions] = None
    ) -> List[PrometheusQueryResult]:
        """
        Execute multiple queries in parallel.

        Args:
            queries: List of PromQL query strings
            options: Query options to apply to all queries

        Returns:
            List of PrometheusQueryResult objects

        Raises:
            PrometheusQueryError: If any query is invalid
        """
        futures = []

        for query in queries:
            futures.append(self.thread_pool.submit(self.query, query, options))

        results = []
        for future in futures:
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"Batch query failed: {str(e)}")
                results.append(None)

        return results

    def stream_query_range(
        self, query: str, options: PrometheusQueryOptions, chunk_size: timedelta
    ) -> Generator[PrometheusQueryResult, None, None]:
        """
        Stream results for a range query in chunks.

        Args:
            query: PromQL query string
            options: Query options
            chunk_size: Time duration for each chunk

        Yields:
            PrometheusQueryResult for each chunk

        Raises:
            PrometheusQueryError: If query is invalid
        """
        self.validate_query(query)

        if not (options.start_time and options.end_time and options.step):
            raise PrometheusValidationError(
                "start_time, end_time, and step are required for range queries"
            )

        current_start = options.start_time
        while current_start < options.end_time:
            current_end = min(current_start + chunk_size, options.end_time)

            chunk_options = PrometheusQueryOptions(
                start_time=current_start,
                end_time=current_end,
                step=options.step,
                timeout=options.timeout,
            )

            yield self.query_range(query, chunk_options)
            current_start = current_end

    def _parse_response(self, response: Dict[str, Any]) -> PrometheusQueryResult:
        """
        Parse Prometheus API response into PrometheusQueryResult.

        Args:
            response: Raw API response

        Returns:
            PrometheusQueryResult containing parsed metrics

        Raises:
            PrometheusAPIError: If response parsing fails
        """
        if response.get("status") != "success":
            raise PrometheusAPIError(
                response.get("error", "Unknown error"), response=response
            )

        result_type = MetricType(response["data"]["resultType"])
        metrics = []

        if result_type in [MetricType.VECTOR, MetricType.MATRIX]:
            for result in response["data"]["result"]:
                metric_name = result["metric"].get("__name__", "")
                labels = {k: v for k, v in result["metric"].items() if k != "__name__"}

                identifier = MetricIdentifier(name=metric_name, labels=labels)

                if result_type == MetricType.VECTOR:
                    metrics.append(
                        PrometheusMetric(
                            identifier=identifier,
                            type=self._get_metric_type(metric_name),
                            samples=[
                                Sample(
                                    timestamp=datetime.fromtimestamp(
                                        result["value"][0]
                                    ),
                                    value=float(result["value"][1]),
                                )
                            ],
                        )
                    )
                else:  # matrix
                    samples = [
                        Sample(
                            timestamp=datetime.fromtimestamp(value[0]),
                            value=float(value[1]),
                        )
                        for value in result["values"]
                    ]
                    metrics.append(
                        PrometheusMetric(
                            identifier=identifier,
                            type=self._get_metric_type(metric_name),
                            samples=samples,
                        )
                    )

        return PrometheusQueryResult(
            result_type=result_type, metrics=metrics, raw_response=response
        )

    def get_metric_metadata(
        self, metric_name: Optional[str] = None
    ) -> List[MetricMetadata]:
        """
        Get metadata about metrics.

        Args:
            metric_name: Optional specific metric name to query

        Returns:
            List of MetricMetadata objects
        """
        params = {}
        if metric_name:
            params["metric"] = metric_name

        response = self._make_request("GET", self.METADATA_ENDPOINT, params=params)

        return [
            MetricMetadata(
                metric_name=metric,
                type=MetricValueType(metadata["type"]),
                help=metadata["help"],
                unit=metadata.get("unit"),
            )
            for metric, metadata in response["data"].items()
        ]

    def _get_metric_type(self, metric_name: str) -> MetricValueType:
        """
        Get the type of a metric by querying its metadata.

        Args:
            metric_name: Name of the metric

        Returns:
            MetricValueType of the metric
        """
        try:
            metadata = self.get_metric_metadata(metric_name)
            if metadata:
                return metadata[0].type
        except Exception as e:
            logger.warning(f"Failed to get metric type for {metric_name}: {str(e)}")
        return MetricValueType.UNTYPED

    def get_targets(self, state: Optional[TargetHealth] = None) -> List[Target]:
        """
        Get information about Prometheus targets.

        Args:
            state: Optional filter by target health state

        Returns:
            List of Target objects
        """
        params = {}
        if state:
            params["state"] = state.value

        response = self._make_request("GET", self.TARGETS_ENDPOINT, params=params)

        targets = []
        for target in response["data"]["activeTargets"]:
            try:
                targets.append(
                    Target(
                        job=target["labels"]["job"],
                        instance=target["labels"]["instance"],
                        health=TargetHealth(target["health"]),
                        labels=target["labels"],
                        last_scrape=datetime.fromisoformat(
                            target["lastScrape"].replace("Z", "+00:00")
                        ),
                        scrape_duration_seconds=float(target["scrapePool"]),
                        error=target.get("lastError", ""),
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to parse target: {str(e)}")
                continue

        return targets

    def get_series(
        self,
        match: List[str],
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[MetricIdentifier]:
        """
        Get time series matching a query.

        Args:
            match: List of series selectors
            start_time: Optional start time
            end_time: Optional end time

        Returns:
            List of MetricIdentifier objects matching the selectors
        """
        params = {"match[]": match}

        if start_time:
            params["start"] = start_time.timestamp()
        if end_time:
            params["end"] = end_time.timestamp()

        response = self._make_request("GET", self.SERIES_ENDPOINT, params=params)

        return [
            MetricIdentifier(
                name=series.get("__name__", ""),
                labels={k: v for k, v in series.items() if k != "__name__"},
            )
            for series in response["data"]
        ]

    def check_query_exemplars(
        self, query: str, start_time: datetime, end_time: datetime
    ) -> bool:
        """
        Check if a query can return exemplars.

        Args:
            query: PromQL query string
            start_time: Start time
            end_time: End time

        Returns:
            bool indicating if query can return exemplars
        """
        try:
            params = {
                "query": query,
                "start": start_time.timestamp(),
                "end": end_time.timestamp(),
            }

            response = self._make_request(
                "GET", "/api/v1/query_exemplars", params=params
            )

            return bool(response.get("data", []))
        except Exception:
            return False

    def health_check(self) -> bool:
        """
        Check if Prometheus is healthy and accessible.

        Returns:
            bool indicating if Prometheus is healthy

        Raises:
            PrometheusConnectionError: If health check fails
        """
        try:
            response = self._make_request("GET", "/-/healthy")
            return response.get("status") == "success"
        except Exception as e:
            raise PrometheusConnectionError(
                "Health check failed",
                url=f"{self.base_url}/-/healthy",
                original_error=e,
            )

    def get_config(self) -> Dict[str, Any]:
        """
        Get Prometheus configuration.

        Returns:
            Dict containing Prometheus configuration

        Raises:
            PrometheusAPIError: If config retrieval fails
        """
        response = self._make_request("GET", "/api/v1/status/config")
        return response["data"]

    def get_flags(self) -> Dict[str, Any]:
        """
        Get Prometheus command-line flags.

        Returns:
            Dict containing Prometheus flags
        """
        response = self._make_request("GET", "/api/v1/status/flags")
        return response["data"]

    def get_build_info(self) -> Dict[str, str]:
        """
        Get Prometheus build information.

        Returns:
            Dict containing build information
        """
        response = self._make_request("GET", "/api/v1/status/buildinfo")
        return response["data"]

    def get_runtimes(self) -> Dict[str, Any]:
        """
        Get Prometheus runtime information.

        Returns:
            Dict containing runtime information
        """
        response = self._make_request("GET", "/api/v1/status/runtimeinfo")
        return response["data"]

    def get_tsdb_status(self) -> Dict[str, Any]:
        """
        Get TSDB status.

        Returns:
            Dict containing TSDB status
        """
        response = self._make_request("GET", "/api/v1/status/tsdb")
        return response["data"]

    def get_wal_replay_status(self) -> Dict[str, Any]:
        """
        Get WAL replay status.

        Returns:
            Dict containing WAL replay status
        """
        response = self._make_request("GET", "/api/v1/status/walreplay")
        return response["data"]

    def __enter__(self) -> "PrometheusClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.session.close()
        self.thread_pool.shutdown(wait=True)

    def close(self):
        """Clean up resources."""
        self.session.close()
        self.thread_pool.shutdown(wait=True)
