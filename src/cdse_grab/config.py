"""
Module: config

Handles configuration management for Sentinel-3 data access.
Supports loading credentials from environment variables or config files.
"""

import json
import logging
import os
from pathlib import Path

import fsspec

# Configure logging
logger = logging.getLogger(__name__)

# Default configuration paths
DEFAULT_CONFIG_PATHS = [
    Path.home() / ".sentinel3" / "config.json",
    Path.cwd() / "sentinel3_config.json",
]


class ConfigurationError(Exception):
    """Exception raised for configuration errors."""

    pass


def get_s3_credentials() -> dict[str, str]:
    """
    Get S3 credentials for Copernicus Data Space.

    First tries environment variables, then falls back to config file.

    Returns:
        Dict with 'endpoint_url', 'access_key', 'secret_key', and
        'https' (bool)

    Raises:
        ConfigurationError: If credentials cannot be found
    """
    # Try environment variables first
    env_creds = _get_credentials_from_env()
    if env_creds:
        logger.debug("Using credentials from environment variables")
        return env_creds

    # Fall back to config file
    file_creds = _get_credentials_from_file()
    if file_creds:
        logger.debug("Using credentials from config file")
        return file_creds

    # No credentials found
    raise ConfigurationError(
        "Could not find S3 credentials. Please set environment variables "
        "(AWS_S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) "
        f"or create a config file in one of: {DEFAULT_CONFIG_PATHS}"
    )


def _get_credentials_from_env() -> dict[str, str] | None:
    """
    Extract S3 credentials from environment variables.

    Returns:
        Dict with credentials if all required variables are set, None
        otherwise
    """
    required_vars = {
        "endpoint_url": "AWS_S3_ENDPOINT",
        "access_key": "AWS_ACCESS_KEY_ID",
        "secret_key": "AWS_SECRET_ACCESS_KEY",
    }

    # Check if all required variables are set
    if not all(os.environ.get(var) for var in required_vars.values()):
        return None

    # Extract credentials
    creds = {
        key: os.environ[env_var] for key, env_var in required_vars.items()
    }

    # Get HTTPS setting (default to True)
    creds["https"] = (
        "YES" if os.environ.get("AWS_HTTPS", "YES").upper() == "YES" else "NO"
    )

    return creds


def _get_credentials_from_file() -> dict[str, str] | None:
    """
    Load S3 credentials from config file.

    Returns:
        Dict with credentials if a valid config file is found, None otherwise
    """
    # Try each potential config path
    for config_path in DEFAULT_CONFIG_PATHS:
        if config_path.exists():
            try:
                with open(config_path, "r") as f:
                    config = json.load(f)

                # Extract credentials
                creds = {
                    "endpoint_url": config.get("s3", {}).get("endpoint_url"),
                    "access_key": config.get("s3", {}).get("access_key"),
                    "secret_key": config.get("s3", {}).get("secret_key"),
                    "https": config.get("s3", {}).get("https", True),
                }

                # Check if all required fields are present
                if all(creds.values()):
                    return creds

            except (json.JSONDecodeError, IOError) as e:
                logger.warning(
                    f"Error reading config file {config_path}: {e}"
                )

    return None


def setup_s3_environment(creds: dict[str, str]) -> None:
    """
    Set up environment variables for S3 access.

    Args:
        creds: Dictionary with S3 credentials
    """
    os.environ["AWS_S3_ENDPOINT"] = creds["endpoint_url"]
    os.environ["AWS_ACCESS_KEY_ID"] = creds["access_key"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = creds["secret_key"]
    os.environ["AWS_HTTPS"] = "YES" if creds["https"] else "NO"
    os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
    os.environ["GDAL_HTTP_UNSAFESSL"] = "YES"


def create_fsspec_filesystem(
    creds: dict[str, str],
) -> fsspec.spec.AbstractFileSystem:
    """
    Create a fsspec filesystem object with the provided credentials.

    Args:
        creds: Dictionary with S3 credentials

    Returns:
        fsspec.filesystem instance
    """

    return fsspec.filesystem(
        "s3",
        anon=False,
        client_kwargs={"endpoint_url": f"https://{creds['endpoint_url']}"},
    )
