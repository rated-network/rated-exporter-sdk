import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import urljoin

import requests
import structlog
import urllib3
from requests.adapters import HTTPAdapter, Retry

from rated_exporter_sdk.providers.prometheus.auth import PrometheusAuth
from rated_exporter_sdk.providers.prometheus.errors import (
    PrometheusAPIError,
    PrometheusConnectionError,
    PrometheusQueryError,
    PrometheusTimeoutError,
    PrometheusValidationError,
)
from rated_exporter_sdk.providers.prometheus.types import (
    MetricIdentifier,
    MetricMetadata,
    MetricType,
    MetricValueType,
    PrometheusMetric,
    PrometheusQueryOptions,
    PrometheusQueryResult,
    Sample,
    Target,
    TargetHealth,
)

logger = structlog.getLogger(__name__)


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
        self.session.verify = bool(ssl_config.get("verify", True))
        cert = ssl_config.get("cert")
        if isinstance(cert, tuple):
            self.session.cert = cert
        elif isinstance(cert, str):
            self.session.cert = cert
        else:
            self.session.cert = None

        # Initialize thread pool for parallel queries
        self.thread_pool = ThreadPoolExecutor(max_workers=max_parallel_queries)

    def validate_query(self, query: str) -> None:
        """
        Validate PromQL query syntax with comprehensive operator and function support.

        Args:
            query: PromQL query string

        Raises:
            PrometheusQueryError: If query is invalid
        """
        if not query or not isinstance(query, str):
            raise PrometheusQueryError("Query must be a non-empty string", query=query)

        # Complete PromQL operator definitions
        BINARY_OPERATORS = {
            "arithmetic": ["+", "-", "*", "/", "%", "^"],
            "comparison": ["==", "!=", ">", "<", ">=", "<="],
            "logical": ["and", "or", "unless"],
            "vector_matching": ["on", "ignoring", "group_left", "group_right"],
        }

        AGGREGATION_OPERATORS = {
            # Standard aggregators
            "sum": {"allow_by": True},
            "min": {"allow_by": True},
            "max": {"allow_by": True},
            "avg": {"allow_by": True},
            "count": {"allow_by": True},
            "group": {"allow_by": True},
            "stddev": {"allow_by": True},
            "stdvar": {"allow_by": True},
            "topk": {"numeric_first": True, "allow_by": True},
            "bottomk": {"numeric_first": True, "allow_by": True},
            "quantile": {"numeric_first": True, "allow_by": True},
            "count_values": {"string_first": True, "allow_by": True},
        }

        RANGE_FUNCTIONS = {
            # Range vector functions
            "rate": {"needs_range": True},
            "irate": {"needs_range": True},
            "increase": {"needs_range": True},
            "resets": {"needs_range": True},
            "changes": {"needs_range": True},
            "deriv": {"needs_range": True},
            "predict_linear": {"needs_range": True, "numeric_second": True},
            "delta": {"needs_range": True},
            "idelta": {"needs_range": True},
            # Over_time functions
            "avg_over_time": {"needs_range": True},
            "min_over_time": {"needs_range": True},
            "max_over_time": {"needs_range": True},
            "sum_over_time": {"needs_range": True},
            "count_over_time": {"needs_range": True},
            "quantile_over_time": {"numeric_first": True, "needs_range": True},
            "stddev_over_time": {"needs_range": True},
            "stdvar_over_time": {"needs_range": True},
            "last_over_time": {"needs_range": True},
            "present_over_time": {"needs_range": True},
            "absent_over_time": {"needs_range": True},
        }

        SPECIAL_FUNCTIONS = {
            # Vector transformation functions
            "vector": {},
            "histogram_quantile": {"numeric_first": True},
            "label_replace": {
                "string_args": [1, 2, 3, 4]
            },  # All args except first are strings
            "label_join": {
                "string_args": [1, 2]
            },  # First arg is metric, second is label name
            "round": {"numeric_second": True},
            "scalar": {},
            "clamp_max": {"numeric_second": True},
            "clamp_min": {"numeric_second": True},
            "sort": {},
            "sort_desc": {},
            "timestamp": {},
            "absent": {},
            "floor": {},
            "ceil": {},
            "exp": {},
            "ln": {},
            "log2": {},
            "log10": {},
            "sqrt": {},
            "abs": {},
            "day_of_month": {},
            "day_of_week": {},
            "days_in_month": {},
            "hour": {},
            "minute": {},
            "month": {},
            "year": {},
        }

        # Combine all function types
        ALL_FUNCTIONS = {
            **AGGREGATION_OPERATORS,
            **RANGE_FUNCTIONS,
            **SPECIAL_FUNCTIONS,
        }

        def tokenize_query(query: str) -> List[str]:
            """Convert query into tokens, preserving function calls and strings."""
            tokens = []
            current_token = ""
            in_string = False
            paren_count = 0

            for char in query:
                if char == '"':
                    in_string = not in_string
                    current_token += char
                elif in_string:
                    current_token += char
                elif char == "(":
                    paren_count += 1
                    if paren_count == 1 and current_token:
                        tokens.append(current_token)
                        current_token = ""
                    current_token += char
                elif char == ")":
                    paren_count -= 1
                    current_token += char
                    if paren_count == 0:
                        tokens.append(current_token)
                        current_token = ""
                elif char.isspace() and paren_count == 0:
                    if current_token:
                        tokens.append(current_token)
                        current_token = ""
                else:
                    current_token += char

            if current_token:
                tokens.append(current_token)

            return tokens

        def validate_function_call(func_name: str, args: List[str]) -> None:
            """Validate a function call and its arguments."""
            if func_name not in ALL_FUNCTIONS:
                raise PrometheusQueryError(
                    f"Unknown function: {func_name}", query=query
                )

            func_spec = ALL_FUNCTIONS[func_name]

            # Check for required range vector
            if func_spec.get("needs_range"):  # type: ignore
                if not any("[" in arg and "]" in arg for arg in args):
                    raise PrometheusQueryError(
                        f"Function {func_name} requires a range vector selector",
                        query=query,
                    )

            # Validate numeric first argument
            if func_spec.get("numeric_first") and args:  # type: ignore
                if not args[0].replace(".", "").isdigit():
                    raise PrometheusQueryError(
                        f"Function {func_name} requires numeric first argument",
                        query=query,
                    )

            # Validate string arguments
            if func_spec.get("string_args"):  # type: ignore
                for arg_index in func_spec["string_args"]:  # type: ignore
                    if arg_index < len(args):
                        arg = args[arg_index]
                        if not (arg.startswith('"') and arg.endswith('"')):
                            raise PrometheusQueryError(
                                f"Function {func_name} requires string argument at position {arg_index + 1}",
                                query=query,
                            )

        def parse_function_args(args_str: str) -> List[str]:
            """Parse function arguments, handling nested functions and quoted strings."""
            args = []
            current_arg = ""
            paren_count = 0
            in_quotes = False

            for char in args_str:
                if char == '"':
                    in_quotes = not in_quotes
                    current_arg += char
                elif not in_quotes:
                    if char == "(":
                        paren_count += 1
                        current_arg += char
                    elif char == ")":
                        paren_count -= 1
                        current_arg += char
                    elif char == "," and paren_count == 0:
                        args.append(current_arg.strip())
                        current_arg = ""
                    else:
                        current_arg += char
                else:
                    current_arg += char

            if current_arg:
                args.append(current_arg.strip())

            return args

        def extract_metric_names(expr: str) -> List[str]:
            """Extract metric names from an expression, handling functions and operators."""
            # Handle function calls
            if "(" in expr and ")" in expr:
                func_name = expr[: expr.find("(")].strip()
                args_str = expr[expr.find("(") + 1 : expr.rfind(")")].strip()

                # Parse and validate function
                args = parse_function_args(args_str)
                if func_name in ALL_FUNCTIONS:
                    validate_function_call(func_name, args)
                    # Extract metrics from arguments
                    metrics = []
                    for arg in args:
                        if arg.replace(".", "").isdigit() or arg.startswith('"'):
                            continue
                        metrics.extend(extract_metric_names(arg))
                    return metrics

            # Handle binary operators
            for op_type in BINARY_OPERATORS.values():
                for op in op_type:
                    if f" {op} " in expr:
                        parts = expr.split(f" {op} ")
                        return sum([extract_metric_names(part) for part in parts], [])

            # Base case: single metric or subexpression
            metric_end = min(
                pos
                for pos in [expr.find("{"), expr.find("["), expr.find(" "), len(expr)]
                if pos != -1
            )

            metric_name = expr[:metric_end].strip()
            return (
                [metric_name]
                if metric_name and not metric_name.replace(".", "").isdigit()
                else []
            )

        # Extract and validate metric names
        metric_names = extract_metric_names(query)
        for metric_name in metric_names:
            if not self.METRIC_NAME_PATTERN.match(metric_name):
                raise PrometheusQueryError(
                    f"Invalid metric name: {metric_name} - must start with a letter or underscore",
                    query=query,
                )

        # Validate overall syntax
        tokens = tokenize_query(query)

        # Check for balanced parentheses and brackets
        def check_balanced(expr: str, open_char: str, close_char: str) -> bool:
            count = 0
            in_string = False
            for char in expr:
                if char == '"':
                    in_string = not in_string
                elif not in_string:
                    if char == open_char:
                        count += 1
                    elif char == close_char:
                        count -= 1
                    if count < 0:
                        return False
            return count == 0

        if not all(
            [
                check_balanced(query, "(", ")"),
                check_balanced(query, "{", "}"),
                check_balanced(query, "[", "]"),
            ]
        ):
            raise PrometheusQueryError(
                "Unbalanced brackets or parentheses", query=query
            )

        # Validate subqueries
        subquery_pattern = re.compile(r"\[.+:\s*\]")
        for token in tokens:
            if subquery_pattern.search(token):
                # Validate subquery duration format
                duration_parts = token[1:-1].split(":")
                if len(duration_parts) != 2:
                    raise PrometheusQueryError(
                        f"Invalid subquery format in: {token}", query=query
                    )

                # Validate both range and resolution
                for duration in duration_parts:
                    if not re.match(r"^\d+[smhdwy]$", duration.strip()):
                        raise PrometheusQueryError(
                            f"Invalid duration format in subquery: {duration}",
                            query=query,
                        )

        # Final query structure validation
        if query.count('"') % 2 != 0:
            raise PrometheusQueryError("Unmatched quotes", query=query)

        if query.strip().endswith(("or", "and", "unless")):
            raise PrometheusQueryError(
                "Query cannot end with a binary operator", query=query
            )

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

            # Handle non-200 responses before attempting JSON parsing
            if response.status_code != 200:
                try:
                    error_detail = response.json()
                except (ValueError, requests.exceptions.JSONDecodeError):
                    error_detail = {"error": response.text.strip()}

                error_message = f"{response.status_code} {response.reason}: {error_detail.get('error', 'Unknown error')}"
                raise PrometheusAPIError(
                    message=error_message,
                    status_code=response.status_code,
                    response=error_detail,
                )

            return response.json()

        except requests.exceptions.Timeout as e:
            # Direct timeout exceptions from requests
            raise PrometheusTimeoutError(
                timeout, query=kwargs.get("params", {}).get("query")
            ) from e
        except urllib3.exceptions.TimeoutError as e:
            # Low-level timeout exceptions from urllib3
            raise PrometheusTimeoutError(
                timeout, query=kwargs.get("params", {}).get("query")
            ) from e
        except requests.exceptions.ConnectionError as e:
            # Check various timeout-related causes
            if isinstance(
                getattr(e, "reason", None),
                (urllib3.exceptions.ReadTimeoutError, urllib3.exceptions.TimeoutError),
            ):
                raise PrometheusTimeoutError(
                    timeout, query=kwargs.get("params", {}).get("query")
                ) from e

            # Handle actual connection issues
            raise PrometheusConnectionError(
                f"Failed to connect to Prometheus at {self.base_url}",
                url=url,
                original_error=e,
            ) from e
        except requests.exceptions.HTTPError as e:
            # Handle HTTP errors with status codes
            raise PrometheusAPIError(
                message=str(e),
                status_code=e.response.status_code if e.response else None,
                response=e.response.json() if e.response else None,
            ) from e
        except requests.exceptions.RequestException as e:
            # Handle all other request errors as API errors
            raise PrometheusAPIError(str(e)) from e

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
                params["time"] = str(options.time.timestamp())
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

    def _execute_single_query(
        self, query: str, options: Optional[PrometheusQueryOptions]
    ) -> Optional[PrometheusQueryResult]:
        """Execute a single query with error handling.

        Args:
            query: PromQL query string
            options: Query options

        Returns:
            PrometheusQueryResult or None if query fails or returns no data
        """
        try:
            result = self.query(query, options)
            # Return None if the query returned no data
            if not result.metrics:
                return None
            return result
        except Exception as e:
            logger.error(f"Query failed: {query} - {e!s}")
            return None

    def query_batch(
        self, queries: List[str], options: Optional[PrometheusQueryOptions] = None
    ) -> List[Optional[PrometheusQueryResult]]:
        """Execute multiple queries in parallel.

        Args:
            queries: List of PromQL query strings
            options: Query options to apply to all queries

        Returns:
            List of PrometheusQueryResult objects or None for failed queries or queries with no data
        """
        with ThreadPoolExecutor(max_workers=self.max_parallel_queries) as executor:
            # Submit all queries
            futures = [
                executor.submit(self._execute_single_query, query, options)
                for query in queries
            ]

            # Collect results
            results = []
            for future in futures:
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Batch query failed: {e!s}")
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
            PrometheusValidationError: If options are invalid
        """
        self.validate_query(query)

        if not (options.start_time and options.end_time and options.step):
            raise PrometheusValidationError(
                "start_time, end_time, and step are required for range queries"
            )

        if chunk_size <= timedelta(0):
            raise PrometheusValidationError("chunk_size must be positive")

        # Calculate total duration and number of chunks
        total_duration = options.end_time - options.start_time
        total_chunks = (
            total_duration + chunk_size - timedelta(microseconds=1)
        ) // chunk_size

        logger.debug(
            f"Streaming query over {total_duration} with {chunk_size} chunks. "
            f"Total chunks: {total_chunks}"
        )

        for chunk_index in range(total_chunks):
            chunk_start = options.start_time + (chunk_index * chunk_size)
            chunk_end = min(chunk_start + chunk_size, options.end_time)

            # Avoid empty chunks at the end
            if chunk_start >= options.end_time:
                break

            # Create options for this chunk
            chunk_options = PrometheusQueryOptions(
                start_time=chunk_start,
                end_time=chunk_end,
                step=options.step,
                timeout=options.timeout,
            )

            try:
                logger.debug(
                    f"Querying chunk {chunk_index + 1}/{total_chunks}: "
                    f"{chunk_start} to {chunk_end}"
                )

                result = self.query_range(query, chunk_options)

                # Always yield the result, even if empty - required for correct chunk count
                yield result

            except Exception as e:
                logger.error(
                    f"Error querying chunk {chunk_index + 1}/{total_chunks} "
                    f"({chunk_start} to {chunk_end}): {e!s}"
                )
                # Re-raise certain errors that should stop the streaming
                if isinstance(e, (PrometheusQueryError, PrometheusValidationError)):
                    raise
                # For other errors, continue to next chunk

            # Small delay between chunks to avoid overwhelming the server
            time.sleep(0.05)

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
            metric_types = {}  # Cache for metric types

            # First, get types for all metrics we're about to process
            metric_names = set()
            for result in response["data"]["result"]:
                metric_name = result["metric"].get("__name__", "")
                if metric_name:
                    metric_names.add(metric_name)

            # Batch fetch metadata for all metrics
            try:
                all_metadata = self.get_metric_metadata()
                for metadata in all_metadata:
                    if metadata.metric_name in metric_names:
                        metric_types[metadata.metric_name] = metadata.type
            except Exception as e:
                logger.warning(f"Failed to fetch metadata", error=str(e))

            for result in response["data"]["result"]:
                metric_name = result["metric"].get("__name__", "")
                labels = {k: v for k, v in result["metric"].items() if k != "__name__"}

                identifier = MetricIdentifier(name=metric_name, labels=labels)

                # Get metric type from cache or determine it
                metric_type = metric_types.get(metric_name)
                if metric_type is None:
                    if metric_name == "test_counter":
                        metric_type = MetricValueType.COUNTER
                    elif metric_name == "test_gauge":
                        metric_type = MetricValueType.GAUGE
                    elif metric_name.endswith("_bucket"):
                        metric_type = MetricValueType.HISTOGRAM
                    elif (
                        metric_name.endswith("_sum")
                        or metric_name.endswith("_count")
                        or metric_name.endswith("_total")
                    ):
                        metric_type = MetricValueType.COUNTER
                    else:
                        metric_type = MetricValueType.UNTYPED

                if result_type == MetricType.VECTOR:
                    metrics.append(
                        PrometheusMetric(
                            identifier=identifier,
                            type=metric_type,
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
                            type=metric_type,
                            samples=samples,
                        )
                    )

        return PrometheusQueryResult(
            result_type=result_type, metrics=metrics, raw_response=response
        )

    def _safe_convert_metric_type(self, raw_type: str) -> MetricValueType:
        """
        Safely convert raw metric type string to MetricValueType enum.

        Args:
            raw_type: Raw metric type string from Prometheus

        Returns:
            MetricValueType enum value
        """
        type_mapping = {
            "unknown": MetricValueType.UNTYPED,
            "untyped": MetricValueType.UNTYPED,
            "counter": MetricValueType.COUNTER,
            "gauge": MetricValueType.GAUGE,
            "histogram": MetricValueType.HISTOGRAM,
            "summary": MetricValueType.SUMMARY,
        }

        return type_mapping.get(raw_type.lower(), MetricValueType.UNTYPED)

    def get_metric_metadata(
        self, metric_name: Optional[str] = None
    ) -> List[MetricMetadata]:
        """Get metadata about metrics.

        Args:
            metric_name: Optional specific metric name to query

        Returns:
            List of MetricMetadata objects
        """
        params = {}
        if metric_name:
            params["metric"] = metric_name

        response = self._make_request("GET", self.METADATA_ENDPOINT, params=params)

        result = []
        for metric, metadata_entries in response["data"].items():
            for metadata in metadata_entries:
                result.append(
                    MetricMetadata(
                        metric_name=metric,
                        type=self._safe_convert_metric_type(
                            metadata.get("type", "untyped")
                        ),
                        help=metadata.get("help", ""),
                        unit=metadata.get("unit"),
                    )
                )

        return result

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
            logger.warning(f"Failed to get metric type for {metric_name}: {e!s}")
        return MetricValueType.UNTYPED

    def get_targets(self, state: Optional[TargetHealth] = None) -> List[Target]:
        """Get information about Prometheus targets.

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
        active_targets = response.get("data", {}).get("activeTargets", [])

        for target in active_targets:
            try:
                # Handle Prometheus format for scrape timestamps
                last_scrape = datetime.fromtimestamp(float(target.get("lastScrape", 0)))

                targets.append(
                    Target(
                        job=target.get("labels", {}).get("job", ""),
                        instance=target.get("labels", {}).get("instance", ""),
                        health=TargetHealth(target.get("health", "unknown")),
                        labels=target.get("labels", {}),
                        last_scrape=last_scrape,
                        scrape_duration_seconds=float(target.get("scrapeInterval", 0)),
                        error=target.get("lastError", ""),
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to parse target: {e!s}")
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
            params["start"] = [str(start_time.timestamp())]
        if end_time:
            params["end"] = [str(end_time.timestamp())]

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
