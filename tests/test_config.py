from pathlib import Path

import pytest

from kingyo_orchestrator.config import Settings, load_settings


def write_config(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "kingyo.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_defaults_require_an_explicit_project(tmp_path):
    path = write_config(tmp_path, '[bigquery]\nproject = "example-project"\n')
    assert load_settings(path) == Settings(project="example-project")


@pytest.mark.parametrize(
    "text, message",
    [
        ("", "bigquery.project"),
        ('[bigquery]\nproject = "  "', "bigquery.project"),
        ("[bigquery]\nproject = 42", "bigquery.project"),
        ('[bigquery]\nproject = "example"\nlocation = ""', "bigquery.location"),
        ('[orchestrator]\nmode = "execute"\n[bigquery]\nproject = "example"', "observe"),
        ('[orchestrator]\npoll_interval_seconds = 0\n[bigquery]\nproject = "example"', "positive"),
        (
            '[orchestrator]\npoll_interval_seconds = true\n[bigquery]\nproject = "example"',
            "positive",
        ),
        ('[bigquery]\nproject = "example"\nlocaton = "US"', "Unknown bigquery"),
        ("[unexpected]\nvalue = 1", "Unknown configuration"),
        ('orchestrator = "observe"', "TOML table"),
    ],
)
def test_rejects_unsafe_or_mistyped_settings(tmp_path, text, message):
    with pytest.raises(ValueError, match=message):
        load_settings(write_config(tmp_path, text))


def test_repository_example_is_valid():
    root = Path(__file__).resolve().parents[1]
    assert load_settings(root / "config" / "kingyo.example.toml").mode == "observe"
