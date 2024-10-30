import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional


class MetricType(str, Enum):
    """Enumeration of Prometheus metric result types."""

    VECTOR = "vector"  # Instant vector: collection of samples at a single time
    MATRIX = "matrix"  # Range vector: collection of samples over time
    SCALAR = "scalar"  # Single numeric value
    STRING = "string"  # String value


class MetricValueType(str, Enum):
    """Enumeration of Prometheus metric value types."""

    COUNTER = "counter"  # Monotonically increasing counter
    GAUGE = "gauge"  # Value that can go up and down
    HISTOGRAM = "histogram"  # Samples observations in buckets
    SUMMARY = "summary"  # Similar to histogram, with quantiles
    UNTYPED = "untyped"  # Type not specified


class TimeUnit(str, Enum):
    """Valid time units for Prometheus step parameter."""

    MILLISECONDS = "ms"
    SECONDS = "s"
    MINUTES = "m"
    HOURS = "h"
    DAYS = "d"
    WEEKS = "w"
    YEARS = "y"


@dataclass(frozen=True)
class Step:
    """
    Represents a time step for range queries.
    Immutable to prevent accidental modifications.
    """

    value: int
    unit: TimeUnit

    def __post_init__(self):
        """Validate step value."""
        if self.value <= 0:
            raise ValueError("Step value must be positive")

    def __str__(self) -> str:
        """Convert to Prometheus step string format."""
        return f"{self.value}{self.unit.value}"

    def to_timedelta(self) -> timedelta:
        """Convert step to timedelta for comparison operations."""
        conversions = {
            TimeUnit.MILLISECONDS: lambda x: timedelta(milliseconds=x),
            TimeUnit.SECONDS: lambda x: timedelta(seconds=x),
            TimeUnit.MINUTES: lambda x: timedelta(minutes=x),
            TimeUnit.HOURS: lambda x: timedelta(hours=x),
            TimeUnit.DAYS: lambda x: timedelta(days=x),
            TimeUnit.WEEKS: lambda x: timedelta(weeks=x),
            TimeUnit.YEARS: lambda x: timedelta(days=x * 365),  # Approximate
        }
        return conversions[self.unit](self.value)


@dataclass(frozen=True)
class Label:
    """
    Represents a Prometheus metric label.
    Immutable to prevent accidental modifications.
    """

    name: str
    value: str

    def __post_init__(self):
        """Validate label name and value."""
        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", self.name):
            raise ValueError(
                f"Invalid label name: {self.name}. Must match regex: ^[a-zA-Z_][a-zA-Z0-9_]*$"
            )


@dataclass
class MetricIdentifier:
    """
    Represents a unique metric identifier in Prometheus.
    Mutable. Use frozen=True for immutability.
    """

    name: str
    labels: dict = field(default_factory=dict)

    def __post_init__(self):
        """Validate metric name."""
        # Handle empty metric names for aggregation functions
        if not self.name and "__name__" in self.labels:
            self.name = self.labels.pop("__name__")

        # Skip validation for aggregation results that might have empty names
        if not self.name:
            return

        if not re.match(r"^[a-zA-Z_:][a-zA-Z0-9_:]*$", self.name):
            raise ValueError(
                f"Invalid metric name: {self.name}. Must match regex: ^[a-zA-Z_:][a-zA-Z0-9_:]*$"
            )


@dataclass(frozen=True)
class Sample:
    """
    Represents a single sample in a time series.
    Immutable to prevent accidental modifications.
    """

    timestamp: datetime
    value: float


@dataclass
class PrometheusMetric:
    """Represents a single Prometheus metric with its time series data."""

    identifier: MetricIdentifier
    type: MetricValueType
    samples: List[Sample]

    @property
    def latest_value(self) -> Optional[float]:
        """Get the most recent sample value."""
        if not self.samples:
            return None
        return max(self.samples, key=lambda s: s.timestamp).value


@dataclass
class PrometheusQueryResult:
    """Represents the result of a Prometheus query."""

    result_type: MetricType
    metrics: List[PrometheusMetric]
    raw_response: Dict[str, Any]

    def __post_init__(self):
        """Validate result type matches metrics structure."""
        if self.result_type == MetricType.SCALAR and len(self.metrics) != 1:
            raise ValueError("Scalar result must contain exactly one metric")
        if self.result_type == MetricType.STRING and len(self.metrics) != 1:
            raise ValueError("String result must contain exactly one metric")


@dataclass
class TimeRange:
    """
    Represents a time range for queries.
    Provides validation and comparison operations.
    """

    start: datetime
    end: datetime

    def __post_init__(self):
        """Validate time range."""
        if self.start > self.end:
            raise ValueError("Start time must be before end time")

    def duration(self) -> timedelta:
        """Get the duration of the time range."""
        return self.end - self.start


@dataclass
class PrometheusQueryOptions:
    """
    Query options for Prometheus queries.

    All time-based parameters accept datetime objects for type safety.
    Step parameter uses the Step class for validation.
    """

    timeout: Optional[float] = None  # seconds
    time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    step: Optional[Step] = None

    # Additional query parameters
    lookback_delta: Optional[Step] = None  # Time duration to look back
    max_source_resolution: Optional[Step] = None  # Maximum source resolution

    def __post_init__(self):
        """Validate query options after initialization."""
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("Timeout must be positive")

        if self.step and not (self.start_time and self.end_time):
            raise ValueError(
                "start_time and end_time are required when step is provided"
            )

        if self.start_time and self.end_time:
            TimeRange(start=self.start_time, end=self.end_time)

        if self.lookback_delta and self.time:
            lookback_time = self.time - self.lookback_delta.to_timedelta()
            if lookback_time > self.time:
                raise ValueError(
                    "Invalid lookback_delta: would result in future timestamp"
                )


@dataclass
class MetricMetadata:
    """Represents metadata about a Prometheus metric."""

    metric_name: str
    type: MetricValueType
    help: str
    unit: Optional[str] = None


class TargetHealth(str, Enum):
    """Enumeration of possible Prometheus target health states."""

    UP = "up"
    DOWN = "down"
    UNKNOWN = "unknown"


@dataclass
class Target:
    """Represents a Prometheus monitoring target."""

    job: str
    instance: str
    health: TargetHealth
    labels: Dict[str, str]
    last_scrape: datetime
    scrape_duration_seconds: float
    error: Optional[str] = None
