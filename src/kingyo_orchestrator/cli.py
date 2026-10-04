"""Local CLI bootstrap. No command connects to cloud services."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from kingyo_orchestrator import __version__
from kingyo_orchestrator.config import load_settings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Kingyo orchestration workspace")
    parser.add_argument("--version", action="version", version=f"kingyo {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check-config", help="Validate settings locally; no cloud calls")
    check.add_argument("path", type=Path, help="Path to a TOML configuration file")
    args = parser.parse_args(argv)

    try:
        settings = load_settings(args.path)
    except (OSError, ValueError) as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        return 2
    print(json.dumps(asdict(settings), indent=2))
    return 0
