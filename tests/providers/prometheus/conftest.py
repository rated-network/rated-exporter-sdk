import base64
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict

import bcrypt
import docker
import pytest
import requests
import yaml
from docker import DockerClient
from docker.errors import APIError, NotFound

from providers.prometheus.helpers.fake_data_generator import (
    generate_test_metrics,
    write_metrics_to_pushgateway,
)

CONTAINER_NAME = "rated-exporter-sdk-prometheus-test"
PUSHGATEWAY_CONTAINER = "rated-exporter-sdk-pushgateway-test"
PUSHGATEWAY_PORT = 9091
PROMETHEUS_PORT = 9090
NETWORK_NAME = "rated-exporter-sdk-test-network"

DEFAULT_AUTH_USERS = {"valid_user": "valid_pass", "test_user": "test_pass"}


def cleanup_networks(client: DockerClient, network_name: str) -> None:
    """Clean up all networks matching the given name."""
    try:
        networks = client.networks.list(names=[network_name])
        for network in networks:
            try:
                # Remove all containers from the network first
                for container in network.containers:
                    try:
                        network.disconnect(container, force=True)
                    except APIError:
                        pass
                # Then remove the network
                network.remove()
            except APIError:
                pass
    except Exception as e:
        print(f"Warning: Error cleaning up networks: {e}")


def cleanup_containers(client: DockerClient, container_names: list) -> None:
    """Clean up containers by name."""
    for name in container_names:
        try:
            container = client.containers.get(name)
            try:
                container.stop(timeout=1)
            except:
                pass
            try:
                container.remove(force=True)
            except:
                pass
        except NotFound:
            pass
        except Exception as e:
            print(f"Warning: Error cleaning up container {name}: {e}")


def wait_for_container_health(container, timeout: int = 30) -> bool:
    """Wait for container to be healthy."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            container.reload()
            status = container.status
            if status == "running":
                health = (
                    container.attrs.get("State", {}).get("Health", {}).get("Status")
                )
                if (
                    health == "healthy" or health is None
                ):  # None means no healthcheck defined
                    return True
        except:
            pass
        time.sleep(1)
    return False


@contextmanager
def docker_network(client: DockerClient, name: str):
    """Context manager for Docker network handling with proper cleanup."""
    # Clean up any existing networks first
    cleanup_networks(client, name)

    try:
        network = client.networks.create(name, driver="bridge")
        yield network
    finally:
        try:
            # Clean up again when done
            cleanup_networks(client, name)
        except Exception:
            pass


class PrometheusTestConfig:
    def __init__(self, with_auth: bool = False):
        self.with_auth = with_auth
        self.auth_users = DEFAULT_AUTH_USERS if with_auth else {}
        self.temp_dir = Path(tempfile.mkdtemp(prefix="prometheus_test_"))
        self.config_path = self.temp_dir / "prometheus.yml"
        self.web_config_path = self.temp_dir / "web.yml"
        self.volumes = {}

    def create_config(self) -> None:
        """Create Prometheus configuration with proper network settings."""
        config = {
            "global": {
                "scrape_interval": "1s",
                "evaluation_interval": "1s",
            },
            "scrape_configs": [
                {
                    "job_name": "pushgateway",
                    "honor_labels": True,
                    "static_configs": [
                        {
                            "targets": [f"{PUSHGATEWAY_CONTAINER}:{PUSHGATEWAY_PORT}"],
                            "labels": {"env": "test"},
                        }
                    ],
                }
            ],
        }

        with open(self.config_path, "w") as f:
            yaml.safe_dump(config, f)

        self.volumes[str(self.config_path.absolute())] = {
            "bind": "/etc/prometheus/prometheus.yml",
            "mode": "ro",
        }

    def create_web_config(self) -> None:
        """Create web configuration file for authentication using bcrypt."""
        if not self.with_auth:
            return

        web_config = {"basic_auth_users": {}}

        for username, password in self.auth_users.items():
            # Generate bcrypt hash
            password_bytes = password.encode("utf-8")
            hashed = bcrypt.hashpw(password_bytes, bcrypt.gensalt(rounds=10))
            web_config["basic_auth_users"][username] = hashed.decode("utf-8")

        with open(self.web_config_path, "w") as f:
            yaml.safe_dump(web_config, f, default_flow_style=False)

        self.volumes[str(self.web_config_path.absolute())] = {
            "bind": "/etc/prometheus/web.yml",
            "mode": "ro",
        }

        # Print web config content for debugging
        with open(self.web_config_path, "r") as f:
            print("Web config content:")
            print(f.read())

    def get_volumes(self) -> Dict[str, Dict[str, str]]:
        """Get all container volume configurations."""
        return self.volumes

    def get_prometheus_args(self) -> list:
        """Get Prometheus command line arguments."""
        args = [
            "--config.file=/etc/prometheus/prometheus.yml",
            "--web.listen-address=:9090",
            "--web.enable-lifecycle",
            "--storage.tsdb.path=/prometheus",
            "--storage.tsdb.retention.time=1h",
        ]

        if self.with_auth:
            args.extend(["--web.config.file=/etc/prometheus/web.yml"])

        return args

    def clean_up(self) -> None:
        """Safely clean up temporary files."""
        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            print(f"Warning: Failed to clean up temp directory: {e}")


@pytest.fixture(scope="session")
def prometheus_environment():
    """
    Enhanced fixture for Prometheus test environment setup.
    """
    client = None
    prometheus_container = None
    pushgateway_container = None
    config = None

    def create_environment(with_auth: bool = False):
        nonlocal client, prometheus_container, pushgateway_container, config

        client = docker.from_env()
        config = PrometheusTestConfig(with_auth=with_auth)

        try:
            # Clean up any existing resources
            cleanup_containers(client, [CONTAINER_NAME, PUSHGATEWAY_CONTAINER])
            cleanup_networks(client, NETWORK_NAME)


            with docker_network(client, NETWORK_NAME):
                # Create configurations first
                config.create_config()
                if with_auth:
                    config.create_web_config()

                # Create and start Pushgateway
                pushgateway_container = client.containers.run(
                    image="prom/pushgateway:latest",
                    name=PUSHGATEWAY_CONTAINER,
                    network=NETWORK_NAME,
                    ports={f"{PUSHGATEWAY_PORT}/tcp": PUSHGATEWAY_PORT},
                    detach=True,
                )

                # Create and start Prometheus with a startup delay
                prometheus_container = client.containers.run(
                    image="prom/prometheus:latest",
                    name=CONTAINER_NAME,
                    network=NETWORK_NAME,
                    ports={f"{PROMETHEUS_PORT}/tcp": PROMETHEUS_PORT},
                    volumes=config.get_volumes(),
                    command=config.get_prometheus_args(),
                    detach=True,
                )

                # Give containers time to initialize
                time.sleep(2)

                # Wait for containers to be healthy
                for container in [pushgateway_container, prometheus_container]:
                    if not wait_for_container_health(container):
                        print(f"\nContainer {container.name} logs:")
                        print(container.logs().decode("utf-8"))
                        raise Exception(
                            f"Container {container.name} failed to become healthy"
                        )

                # Verify services are responding
                base_url = f"http://localhost:{PROMETHEUS_PORT}"
                auth = ("valid_user", "valid_pass") if with_auth else None

                max_retries = 20  # Increase retry attempts
                retry_delay = 1.5  # Increase delay between retries

                for attempt in range(max_retries):
                    try:
                        response = requests.get(
                            f"{base_url}/-/ready",
                            auth=auth,
                            timeout=2,  # Increase timeout
                        )
                        if response.status_code == 200:
                            print(f"Service check successful on attempt {attempt + 1}")
                            return {
                                "url": base_url,
                                "pushgateway_url": f"http://localhost:{PUSHGATEWAY_PORT}",
                                "prometheus": prometheus_container,
                                "pushgateway": pushgateway_container,
                                "auth_enabled": with_auth,
                                "auth_users": config.auth_users if with_auth else {},
                            }
                        print(
                            f"Attempt {attempt + 1}: Service check failed with status code: {response.status_code}"
                        )
                    except requests.exceptions.RequestException as e:
                        print(
                            f"Attempt {attempt + 1}: Service check failed with error: {e!s}"
                        )

                    if prometheus_container.status != "running":
                        print("Prometheus container stopped running!")
                        print(prometheus_container.logs().decode("utf-8"))
                        break

                    time.sleep(retry_delay)

                # Print final container logs before raising exception
                for container in [prometheus_container, pushgateway_container]:
                    print(f"\n{container.name} final logs:")
                    print(container.logs().decode("utf-8"))

                raise Exception("Services failed to become ready")

        except Exception as e:
            # Clean up on failure
            for container in [prometheus_container, pushgateway_container]:
                if container:
                    try:
                        container.remove(force=True)
                    except:
                        pass
            if config:
                config.clean_up()
            raise e

    return create_environment


@pytest.fixture(scope="session")
def prometheus_with_data(prometheus_environment):
    """Provides a Prometheus environment with test data loaded."""
    env = prometheus_environment(with_auth=False)
    try:
        # Write test data
        write_metrics_to_pushgateway(env["pushgateway_url"], generate_test_metrics())

        # Wait for data to be scraped
        auth = None
        if env.get("auth_enabled"):
            auth = ("valid_user", "valid_pass")

        # Verify data availability
        for _ in range(10):
            try:
                kwargs = {"auth": auth} if auth else {}
                response = requests.get(
                    f"{env['url']}/api/v1/query",
                    params={"query": "test_counter"},
                    **kwargs,
                )
                if response.status_code == 200 and response.json().get("data", {}).get(
                    "result", []
                ):
                    return env
            except requests.exceptions.RequestException:
                pass
            time.sleep(1)

        pytest.skip("Failed to verify test data in Prometheus")
    except Exception as e:
        pytest.skip(f"Failed to set up test data: {e!s}")


@pytest.fixture
def mock_prometheus(requests_mock) -> Dict[str, str]:
    """Provides a mocked Prometheus API for testing."""
    base_url = "http://mock-prometheus:9090"

    # Mock instant query
    def query_response(request):
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Basic "):
            credentials = base64.b64decode(auth_header[6:]).decode()
            username, password = credentials.split(":")
            if username == "valid_user" and password == "valid_pass":
                return {
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [
                            {
                                "metric": {"__name__": "test_metric"},
                                "value": [1634000000, "42"],
                            }
                        ],
                    },
                }
        return {"status": "error", "error": "Unauthorized"}

    requests_mock.get(f"{base_url}/api/v1/query", json=query_response)
    return {"base_url": base_url}
