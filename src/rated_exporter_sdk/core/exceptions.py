class RatedExporterBaseError(Exception):
    """Base exception class for Rated Exporter SDK."""

    pass


class AuthenticationError(RatedExporterBaseError):
    """Raised when there's an authentication problem."""

    pass


class ConfigurationError(RatedExporterBaseError):
    """Raised when there's a configuration issue."""

    pass


class APIError(RatedExporterBaseError):
    """Raised when there's an error in API communication."""

    pass


class DataSourceError(RatedExporterBaseError):
    """Raised when there's an issue with a specific data source."""

    pass


class QueryError(RatedExporterBaseError):
    """Raised when there's an error in query execution or parsing."""

    pass
