from datetime import datetime, timedelta

import pytest
from rated_exporter_sdk.providers.prometheus.types import (
    Label,
    MetricIdentifier,
    MetricMetadata,
    MetricType,
    MetricValueType,
    PrometheusMetric,
    PrometheusQueryOptions,
    PrometheusQueryResult,
    Sample,
    Step,
    Target,
    TargetHealth,
    TimeRange,
    TimeUnit,
)


@pytest.mark.provider("prometheus")
class TestPrometheusTypes:
    """Test suite for Prometheus type classes."""

    def test_step_validation(self):
        """Test Step class validation."""
        # Valid steps
        step = Step(value=15, unit=TimeUnit.SECONDS)
        assert str(step) == "15s"

        step = Step(value=1, unit=TimeUnit.MINUTES)
        assert str(step) == "1m"

        # Invalid steps
        with pytest.raises(ValueError):
            Step(value=0, unit=TimeUnit.SECONDS)
        with pytest.raises(ValueError):
            Step(value=-1, unit=TimeUnit.SECONDS)

    def test_step_timedelta_conversion(self):
        """Test Step to timedelta conversion."""
        conversions = [
            (
                Step(value=1000, unit=TimeUnit.MILLISECONDS),
                timedelta(milliseconds=1000),
            ),
            (Step(value=30, unit=TimeUnit.SECONDS), timedelta(seconds=30)),
            (Step(value=5, unit=TimeUnit.MINUTES), timedelta(minutes=5)),
            (Step(value=2, unit=TimeUnit.HOURS), timedelta(hours=2)),
            (Step(value=1, unit=TimeUnit.DAYS), timedelta(days=1)),
            (Step(value=1, unit=TimeUnit.WEEKS), timedelta(weeks=1)),
            (Step(value=1, unit=TimeUnit.YEARS), timedelta(days=365)),
        ]

        for step, expected_delta in conversions:
            assert step.to_timedelta() == expected_delta

    def test_label_validation(self):
        """Test Label class validation."""
        # Valid labels
        Label(name="valid_label", value="value")
        Label(name="_valid_label", value="value")
        Label(name="a", value="value")

        # Invalid labels
        invalid_names = [
            "invalid-label",  # Contains hyphen
            "1invalid",  # Starts with number
            "",  # Empty string
            "invalid!",  # Special character
            "über",  # Non-ASCII
        ]

        for invalid_name in invalid_names:
            with pytest.raises(ValueError):
                Label(name=invalid_name, value="value")

    def test_metric_identifier_validation(self):
        """Test MetricIdentifier class validation."""
        # Valid identifiers
        MetricIdentifier(name="valid_metric")
        MetricIdentifier(name="valid:metric")
        MetricIdentifier(name="valid_metric", labels={"valid_label": "value"})

        # Invalid metric names
        invalid_metrics = [
            "invalid-metric",  # Contains hyphen
            "1invalid",  # Starts with number
            "",  # Empty string
            "invalid!",  # Special character
        ]

        for invalid_metric in invalid_metrics:
            with pytest.raises(ValueError):
                MetricIdentifier(name=invalid_metric)

        # Invalid label names
        with pytest.raises(ValueError):
            MetricIdentifier(name="valid_metric", labels={"invalid-label": "value"})

    def test_query_options_validation(self):
        """Test PrometheusQueryOptions validation."""
        now = datetime.now()

        # Valid options
        opts = PrometheusQueryOptions(
            start_time=now - timedelta(hours=1),
            end_time=now,
            step=Step(value=15, unit=TimeUnit.SECONDS),
            timeout=30.0,
            lookback_delta=Step(value=5, unit=TimeUnit.MINUTES),
        )
        assert opts.timeout == 30.0

        # Invalid timeout
        with pytest.raises(ValueError):
            PrometheusQueryOptions(timeout=-1)

        # Invalid time range
        with pytest.raises(ValueError):
            PrometheusQueryOptions(start_time=now, end_time=now - timedelta(hours=1))

        # Missing required fields with step
        with pytest.raises(ValueError):
            PrometheusQueryOptions(step=Step(value=15, unit=TimeUnit.SECONDS))

    def test_time_range(self):
        """Test TimeRange class functionality."""
        now = datetime.now()
        start = now - timedelta(hours=1)

        # Valid range
        time_range = TimeRange(start=start, end=now)
        assert time_range.duration() == timedelta(hours=1)

        # Invalid range
        with pytest.raises(ValueError):
            TimeRange(start=now, end=start)

    def test_sample_creation(self):
        """Test Sample class functionality."""
        now = datetime.now()
        sample = Sample(timestamp=now, value=42.0)

        assert sample.timestamp == now
        assert sample.value == 42.0

    def test_prometheus_metric(self):
        """Test PrometheusMetric class functionality."""
        now = datetime.now()
        identifier = MetricIdentifier(name="test_metric")
        samples = [
            Sample(timestamp=now, value=42.0),
            Sample(timestamp=now + timedelta(minutes=1), value=43.0),
        ]

        metric = PrometheusMetric(
            identifier=identifier, type=MetricValueType.COUNTER, samples=samples
        )

        assert metric.latest_value == 43.0
        assert len(metric.samples) == 2
        assert metric.type == MetricValueType.COUNTER

        # Test empty samples
        empty_metric = PrometheusMetric(
            identifier=identifier, type=MetricValueType.GAUGE, samples=[]
        )
        assert empty_metric.latest_value is None

    def test_query_result_validation(self):
        """Test PrometheusQueryResult validation."""
        identifier = MetricIdentifier(name="test_metric")
        metric = PrometheusMetric(
            identifier=identifier,
            type=MetricValueType.COUNTER,
            samples=[Sample(timestamp=datetime.now(), value=42.0)],
        )

        # Valid vector result
        result = PrometheusQueryResult(
            result_type=MetricType.VECTOR,
            metrics=[metric],
            raw_response={"status": "success"},
        )
        assert result.result_type == MetricType.VECTOR

        # Invalid scalar result (should have exactly one metric)
        with pytest.raises(ValueError):
            PrometheusQueryResult(
                result_type=MetricType.SCALAR, metrics=[metric, metric], raw_response={}
            )

        # Invalid string result (should have exactly one metric)
        with pytest.raises(ValueError):
            PrometheusQueryResult(
                result_type=MetricType.STRING, metrics=[], raw_response={}
            )

    def test_target_health_enum(self):
        """Test TargetHealth enum functionality."""
        assert TargetHealth.UP.value == "up"
        assert TargetHealth.DOWN.value == "down"
        assert TargetHealth.UNKNOWN.value == "unknown"

        # Test conversion from string
        assert TargetHealth("up") == TargetHealth.UP
        assert TargetHealth("down") == TargetHealth.DOWN
        assert TargetHealth("unknown") == TargetHealth.UNKNOWN

        with pytest.raises(ValueError):
            TargetHealth("invalid")

    def test_metric_metadata(self):
        """Test MetricMetadata class functionality."""
        metadata = MetricMetadata(
            metric_name="test_metric",
            type=MetricValueType.COUNTER,
            help="Test metric help text",
            unit="seconds",
        )

        assert metadata.metric_name == "test_metric"
        assert metadata.type == MetricValueType.COUNTER
        assert metadata.help == "Test metric help text"
        assert metadata.unit == "seconds"

    def test_target(self):
        """Test Target class functionality."""
        now = datetime.now()
        target = Target(
            job="test_job",
            instance="localhost:9090",
            health=TargetHealth.UP,
            labels={"env": "test"},
            last_scrape=now,
            scrape_duration_seconds=0.1,
            error="",
        )

        assert target.job == "test_job"
        assert target.instance == "localhost:9090"
        assert target.health == TargetHealth.UP
        assert target.labels == {"env": "test"}
        assert target.last_scrape == now
        assert target.scrape_duration_seconds == 0.1
        assert target.error == ""
