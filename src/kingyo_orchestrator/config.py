"""Read local settings without importing cloud clients or loading credentials."""

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    project: str
    location: str = "US"
    poll_interval_seconds: int = 300
    mode: str = "observe"


def load_settings(path: Path) -> Settings:
    """Validate the observe-only configuration supported by the starter."""
    with path.open("rb") as stream:
        data = tomllib.load(stream)
    unknown = data.keys() - {"orchestrator", "bigquery"}
    if unknown:
        raise ValueError(f"Unknown configuration sections: {', '.join(sorted(unknown))}")

    orchestrator = data.get("orchestrator", {})
    bigquery = data.get("bigquery", {})
    for name, section, allowed in (
        ("orchestrator", orchestrator, {"mode", "poll_interval_seconds"}),
        ("bigquery", bigquery, {"project", "location"}),
    ):
        if not isinstance(section, dict):
            raise ValueError(f"{name} must be a TOML table")
        unknown = section.keys() - allowed
        if unknown:
            raise ValueError(f"Unknown {name} settings: {', '.join(sorted(unknown))}")

    project = bigquery.get("project")
    location = bigquery.get("location", "US")
    interval = orchestrator.get("poll_interval_seconds", 300)
    mode = orchestrator.get("mode", "observe")
    if not isinstance(project, str) or not project.strip():
        raise ValueError("bigquery.project must be a nonempty string")
    if not isinstance(location, str) or not location.strip():
        raise ValueError("bigquery.location must be a nonempty string")
    if type(interval) is not int or interval <= 0:
        raise ValueError("orchestrator.poll_interval_seconds must be a positive integer")
    if mode != "observe":
        raise ValueError("Only orchestrator.mode = 'observe' is supported")
    return Settings(project.strip(), location.strip(), interval, mode)
