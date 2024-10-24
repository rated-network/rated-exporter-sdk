import pytest
from pathlib import Path
from typing import Set
import subprocess


def get_changed_files() -> Set[str]:
    """
    Get list of files changed since last commit.
    Supports both uncommitted changes and committed changes not pushed.

    Returns:
        Set of changed file paths
    """
    changed_files = set()

    try:
        # Get uncommitted changes
        result = subprocess.run(
            ["git", "diff", "--name-only"], capture_output=True, text=True
        )
        if result.stdout:
            changed_files.update(result.stdout.splitlines())

        # Get staged changes
        result = subprocess.run(
            ["git", "diff", "--cached", "--name-only"], capture_output=True, text=True
        )
        if result.stdout:
            changed_files.update(result.stdout.splitlines())

        # Get committed but not pushed changes
        result = subprocess.run(
            ["git", "diff", "--name-only", "origin/main...HEAD"],
            capture_output=True,
            text=True,
        )
        if result.stdout:
            changed_files.update(result.stdout.splitlines())

    except subprocess.SubprocessError:
        # If git commands fail, run all tests
        return set()

    return {str(Path(f).resolve()) for f in changed_files}


def get_provider_from_path(path: str) -> str:
    """
    Extract provider name from file path.

    Args:
        path: File path

    Returns:
        Provider name or None if not in a provider directory
    """
    path_parts = Path(path).parts
    if "providers" in path_parts:
        provider_index = path_parts.index("providers") + 1
        if len(path_parts) > provider_index:
            return path_parts[provider_index]
    return None


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "provider(name): mark test to run only when specific provider files change",
    )


def pytest_collection_modifyitems(config, items):
    """
    Pytest hook to modify test collection based on changed files.
    Skips tests unless their related provider files have changed.
    """
    changed_files = get_changed_files()
    if not changed_files:
        return  # If we can't determine changes, run all tests

    changed_providers = {
        provider for path in changed_files if (provider := get_provider_from_path(path))
    }

    # If no provider files changed, run all tests
    if not changed_providers:
        return

    for item in items:
        provider_markers = [marker for marker in item.iter_markers(name="provider")]

        # If test has no provider marker, it runs regardless of changes
        if not provider_markers:
            continue

        # Skip test if its provider(s) haven't changed
        if not any(marker.args[0] in changed_providers for marker in provider_markers):
            skip_marker = pytest.mark.skip(
                reason=f"No changes in provider: {[m.args[0] for m in provider_markers]}"
            )
            item.add_marker(skip_marker)


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--provider",
        action="store",
        default=None,
        help="Run tests for specific provider only",
    )
