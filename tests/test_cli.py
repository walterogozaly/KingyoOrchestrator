import json

from kingyo_orchestrator.cli import main


def test_check_config_returns_structured_settings(tmp_path, capsys):
    path = tmp_path / "settings.toml"
    path.write_text('[bigquery]\nproject = "example-project"\n', encoding="utf-8")
    assert main(["check-config", str(path)]) == 0
    assert json.loads(capsys.readouterr().out)["project"] == "example-project"


def test_missing_file_returns_actionable_error(tmp_path, capsys):
    assert main(["check-config", str(tmp_path / "missing.toml")]) == 2
    result = capsys.readouterr()
    assert result.out == ""
    assert "Configuration error:" in result.err


def test_invalid_toml_has_no_traceback(tmp_path, capsys):
    path = tmp_path / "broken.toml"
    path.write_text("[broken", encoding="utf-8")
    assert main(["check-config", str(path)]) == 2
    assert "Configuration error:" in capsys.readouterr().err
