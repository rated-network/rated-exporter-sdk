from datetime import datetime
from typing import Optional

import google.auth
from google.auth import impersonated_credentials
from google.auth.credentials import Credentials
from google.auth.transport.requests import Request
from google.oauth2 import service_account

from rated_exporter_sdk.core.exceptions import AuthenticationError
from rated_exporter_sdk.providers.prometheus.auth import PrometheusAuth


class GCPPrometheusAuth(PrometheusAuth):
    """Extended PrometheusAuth with GCP token refresh capabilities"""

    def __init__(
        self,
        credentials: Optional[Credentials] = None,
        service_account_file: Optional[str] = None,
        target_principal: Optional[str] = None,
        token_lifetime: int = 3600,
        *args,
        **kwargs
    ):
        self.credentials: Optional[Credentials] = credentials
        self.service_account_file = service_account_file
        self.target_principal = target_principal
        self.token_lifetime = token_lifetime
        self.scopes = ["https://www.googleapis.com/auth/monitoring.read"]
        self._setup_credentials()

        if self.credentials is None:
            raise AuthenticationError(
                "Failed to initialize Prometheus client with GCP cloud credentials"
            )

        current_token = self._get_fresh_token()
        super().__init__(token=current_token, *args, **kwargs)  # type: ignore

    def _setup_credentials(self):
        """Set up GCP credentials"""
        if not self.credentials:
            if self.service_account_file:
                # Load from service account file
                base_credentials = (
                    service_account.Credentials.from_service_account_file(
                        self.service_account_file, scopes=self.scopes
                    )
                )
            else:
                # Try to load default credentials
                base_credentials, _ = google.auth.default(scopes=self.scopes)

            if self.target_principal:
                # Set up service account impersonation if needed
                self.credentials = impersonated_credentials.Credentials(
                    source_credentials=base_credentials,
                    target_principal=self.target_principal,
                    target_scopes=self.scopes,
                    lifetime=self.token_lifetime,
                )
            else:
                self.credentials = base_credentials

    def _get_fresh_token(self) -> str:
        """Get a fresh token, refreshing if necessary"""
        if self.credentials is None:
            raise AuthenticationError("Credentials not initialized")

        if not self.credentials.valid:
            self.credentials.refresh(Request())

        elif (
            self.credentials.expiry
            and (self.credentials.expiry - datetime.now()).total_seconds() < 300
        ):
            # Proactively refresh if less than 5 minutes until expiry
            self.credentials.refresh(Request())

        if not self.credentials.token:
            raise AuthenticationError("No token available after refresh")

        return self.credentials.token

    def get_auth_headers(self) -> dict:
        """Override to ensure fresh token"""
        self.token = self._get_fresh_token()
        return super().get_auth_headers()
