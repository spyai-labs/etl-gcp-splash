from typing import Dict
from splash.config.settings import Settings


def get_proxy() -> Dict[str, str]:
    """
    Constructs a dictionary of proxy settings from application configuration.

    Returns:
        Dict[str, str]: Dictionary with `http`, `https`, and `no_proxy` entries
                        sourced from `Settings`.
    """
    return {
        "http": Settings.HTTP_PROXY,
        "https": Settings.HTTP_PROXY,
        "no_proxy": Settings.NO_PROXY
    }
