"""
Configuration loader for Argus.

Reads YAML config based on ARGUS_ENV (default: dev).
Supports ${ENV_VAR} interpolation for production secrets.
"""

import os
import re
from pathlib import Path
from typing import Any

import yaml


def _resolve_env_vars(value: str) -> str:
    """Replace ${VAR_NAME} placeholders with environment variable values."""
    pattern = re.compile(r"\$\{([^}]+)\}")
    def replacer(match: re.Match) -> str:
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))
    return pattern.sub(replacer, value)


def _walk_and_resolve(obj: Any) -> Any:
    """Recursively resolve env vars in all string values."""
    if isinstance(obj, str):
        return _resolve_env_vars(obj)
    elif isinstance(obj, dict):
        return {k: _walk_and_resolve(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_walk_and_resolve(item) for item in obj]
    return obj


class ArgusConfig:
    """Central configuration object for Argus."""

    def __init__(self, config_dict: dict[str, Any]):
        self._raw = config_dict
        self.llm = config_dict.get("llm", {})
        self.logging = config_dict.get("logging", {})
        self.pipeline = config_dict.get("pipeline", {})
        self.agents = config_dict.get("agents", {})
        self.output = config_dict.get("output", {})

    def get(self, dotted_key: str, default: Any = None) -> Any:
        """Access nested config via dot notation: 'llm.provider'."""
        keys = dotted_key.split(".")
        value = self._raw
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return default
            if value is None:
                return default
        return value


def load_config(env: str | None = None) -> ArgusConfig:
    """
    Load configuration for the given environment.

    Args:
        env: One of 'dev', 'staging', 'prod'. Defaults to ARGUS_ENV or 'dev'.

    Returns:
        ArgusConfig instance with resolved environment variables.
    """
    env = env or os.environ.get("ARGUS_ENV", "dev")
    config_dir = Path(__file__).parent.parent.parent / "configs" / env
    config_path = config_dir / "config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    resolved = _walk_and_resolve(raw)
    return ArgusConfig(resolved)
