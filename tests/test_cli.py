from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from click.testing import CliRunner

import zbdump as zbdump_module


@contextmanager
def _fake_connection() -> object:
    yield object()


def test_cli_uses_default_output_path_when_output_file_is_missing(
    monkeypatch,
) -> None:
    table_info = zbdump_module.TableInfo(
        table_name="dump_fixture",
        columns=[
            zbdump_module.TableColumnInfo(
                ordinal_position=1,
                column_name="id",
                rendered_data_type="integer",
                is_nullable=False,
                column_default=None,
                is_identity=False,
                identity_generation=None,
            )
        ],
        indices=[],
        primary_key_columns=["id"],
    )

    monkeypatch.setattr(
        zbdump_module, "get_database_connection", lambda _: _fake_connection()
    )
    monkeypatch.setattr(
        zbdump_module.DatabaseConnectionProtocol,
        "read_table_config",
        lambda self, table_name: table_info,
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            zbdump_module.zbdump,
            ["--database_url", "postgresql://ignored", "dump_fixture"],
        )

        output_path = Path("dump_fixture_dump.sql").absolute()
        assert result.exit_code == 0, result.output
        assert output_path.exists()
        assert 'CREATE TABLE "dump_fixture"' in output_path.read_text(encoding="utf-8")
