from click.testing import CliRunner

from spark_insight.cli import cli


def test_cli_lists_core_commands():
    result = CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "apps" in result.output
    assert "diff" in result.output
    assert "upload" in result.output
    assert "serve" in result.output


def test_diff_command_help():
    result = CliRunner().invoke(cli, ["diff", "--help"])

    assert result.exit_code == 0
    assert "Compare two applications" in result.output
