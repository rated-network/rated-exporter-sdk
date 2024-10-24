import logging
import requests
import time
from typing import Optional, Tuple, Dict
from datetime import datetime


class ServiceHealthCheck:
    """Enhanced service health checking with detailed logging and diagnostics."""

    def __init__(self, timeout: int = 30, check_interval: float = 1.0):
        self.timeout = timeout
        self.check_interval = check_interval
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Set up a logger with appropriate formatting."""
        logger = logging.getLogger("ServiceHealthCheck")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def check_services(
        self,
        prometheus_url: str,
        pushgateway_port: int,
        auth: Optional[Tuple[str, str]] = None,
    ) -> Tuple[bool, Dict[str, str]]:
        """
        Check if Prometheus and Pushgateway services are ready.

        Returns:
            Tuple[bool, Dict[str, str]]: (success, diagnostic_info)
        """
        start_time = datetime.now()
        diagnostic_info = {
            "prometheus_status": "Not checked",
            "pushgateway_status": "Not checked",
            "total_duration": "0s",
            "last_error": None,
        }

        self.logger.info(f"Starting service health check - Timeout: {self.timeout}s")

        kwargs = {"auth": auth} if auth else {}
        attempt = 0

        while (datetime.now() - start_time).total_seconds() < self.timeout:
            attempt += 1
            self.logger.info(f"Attempt {attempt} - Checking services...")

            try:
                # Check Prometheus
                prom_response = requests.get(
                    f"{prometheus_url}/-/ready", timeout=2, **kwargs
                )
                diagnostic_info["prometheus_status"] = (
                    f"OK ({prom_response.status_code})"
                    if prom_response.status_code == 200
                    else f"Failed ({prom_response.status_code})"
                )

                # Check Pushgateway
                push_response = requests.get(
                    f"http://localhost:{pushgateway_port}/-/ready", timeout=2
                )
                diagnostic_info["pushgateway_status"] = (
                    f"OK ({push_response.status_code})"
                    if push_response.status_code == 200
                    else f"Failed ({push_response.status_code})"
                )

                if (
                    prom_response.status_code == 200
                    and push_response.status_code == 200
                ):
                    duration = (datetime.now() - start_time).total_seconds()
                    diagnostic_info["total_duration"] = f"{duration:.1f}s"

                    # Add stabilization delay
                    self.logger.info(
                        "Services responded successfully. Adding stabilization delay..."
                    )
                    time.sleep(2)

                    self.logger.info(
                        "Service health check completed successfully:\n"
                        + "\n".join(f"{k}: {v}" for k, v in diagnostic_info.items())
                    )
                    return True, diagnostic_info

            except requests.exceptions.ConnectionError as e:
                diagnostic_info["last_error"] = f"Connection error: {str(e)}"
                self.logger.warning(f"Connection failed: {str(e)}")
            except requests.exceptions.ReadTimeout as e:
                diagnostic_info["last_error"] = f"Timeout error: {str(e)}"
                self.logger.warning(f"Request timed out: {str(e)}")
            except Exception as e:
                diagnostic_info["last_error"] = f"Unexpected error: {str(e)}"
                self.logger.error(f"Unexpected error during health check: {str(e)}")

            time.sleep(self.check_interval)

        duration = (datetime.now() - start_time).total_seconds()
        diagnostic_info["total_duration"] = f"{duration:.1f}s"
        self.logger.error(
            "Service health check failed. Final status:\n"
            + "\n".join(f"{k}: {v}" for k, v in diagnostic_info.items())
        )
        return False, diagnostic_info
