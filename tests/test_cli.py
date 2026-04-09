from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from click.testing import CliRunner

import zbdump as zbdump_module


@contextmanager
def _fake_connection() -> Iterator[object]:
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
    monkeypatch.setattr(
        zbdump_module.DatabaseConnectionProtocol,
        "iter_table_rows",
        lambda self, table_info: iter([("1",)]),
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
        dump_text = output_path.read_text(encoding="utf-8")
        assert 'CREATE TABLE "dump_fixture"' in dump_text
        assert 'INSERT INTO "dump_fixture" VALUES (1);' in dump_text


def test_cli_passes_output_file_as_path_instance(monkeypatch) -> None:
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
    seen_output_path: Path | None = None

    monkeypatch.setattr(
        zbdump_module, "get_database_connection", lambda _: _fake_connection()
    )
    monkeypatch.setattr(
        zbdump_module.DatabaseConnectionProtocol,
        "read_table_config",
        lambda self, table_name: table_info,
    )
    monkeypatch.setattr(
        zbdump_module.DatabaseConnectionProtocol,
        "iter_table_rows",
        lambda self, table_info: iter([("1",)]),
    )

    class FakeRenderer:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def render_table_ddl(self, table_info):
            return None

        def render_table_data(self, table_info, rows, *, include_column_names):
            return None

        def render_table_indexes(self, table_info):
            return None

    def fake_from_path(output_path: Path) -> FakeRenderer:
        nonlocal seen_output_path
        seen_output_path = output_path
        return FakeRenderer()

    monkeypatch.setattr(zbdump_module.DumpFileRenderer, "from_path", fake_from_path)

    result = CliRunner().invoke(
        zbdump_module.zbdump,
        [
            "--database_url",
            "postgresql://ignored",
            "--output_file",
            "custom_dump.sql",
            "dump_fixture",
        ],
    )

    assert result.exit_code == 0, result.output
    assert isinstance(seen_output_path, Path)
    assert seen_output_path == Path("custom_dump.sql")
