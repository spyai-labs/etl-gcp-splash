import os
from pathlib import Path
from dotenv import load_dotenv
from typing import cast, TYPE_CHECKING

if TYPE_CHECKING:
    from splash.defined_types import SyncMode

# Supported sync modes for validation
VALID_SYNC_MODES = {"incremental", "incremental_window", "historical_full"}

# Required secrets to validate presence before running the app
REQUIRED_SECRETS = ["CLIENT_ID", "CLIENT_SECRET", "USERNAME", "PASSWORD"]


def load_env() -> None:
    """
    Load environment variables from a `.env` file located two levels up from this file.
    This is especially useful during local development.
    """
    dotenv_path = Path(__file__).resolve().parents[2] / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
        print(".env file loaded")
    else:
        print(".env file not found")


def get_sync_mode(default: str = "incremental") -> 'SyncMode':
    """
    Read and validate the SYNC_MODE environment variable.

    Returns:
        SyncMode: A valid sync mode literal ('incremental', 'incremental_window', 'historical_full').
    Raises:
        ValueError: If SYNC_MODE is invalid.
    """
    mode = os.getenv("SYNC_MODE", default).lower()
    if mode not in VALID_SYNC_MODES:
        raise ValueError(f"Invalid SYNC_MODE: {mode}")
    return cast('SyncMode', mode)


def check_secrets() -> None:
    """
    Check that all required secret variables are defined in the environment.
    
    Raises:
        RuntimeError: If any required secrets are missing.
    """
    missing = [var for var in REQUIRED_SECRETS if not os.getenv(var)]
    if missing:
        raise RuntimeError(f"Missing required secrets: {', '.join(missing)}")
