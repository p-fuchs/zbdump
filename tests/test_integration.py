from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from click.testing import CliRunner
from testcontainers.postgres import PostgresContainer

from zbdump import zbdump


SEED_SQL_PATH = Path(__file__).resolve().parents[1] / "docker" / "initdb" / "001_fixture.sql"


def _replace_database_name(database_url: str, database_name: str) -> str:
    split_result = urlsplit(database_url)
    updated_result = SplitResult(
        scheme=split_result.scheme,
        netloc=split_result.netloc,
        path=f"/{database_name}",
        query=split_result.query,
        fragment=split_result.fragment,
    )
    return urlunsplit(updated_result)


def _create_database(admin_url: str, database_name: str) -> str:
    with psycopg.connect(admin_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')
            cursor.execute(f'CREATE DATABASE "{database_name}"')

    return _replace_database_name(admin_url, database_name)


def _restore_dump(database_url: str, dump_path: Path) -> None:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(dump_path.read_text(encoding="utf-8"))
        connection.commit()


@pytest.fixture(scope="module")
def postgres_container() -> Iterator[PostgresContainer]:
    with PostgresContainer(
        "postgres:17",
        driver=None,
        username="zbdump",
        password="zbdump",
        dbname="zbdump_fixture",
    ) as postgres:
        yield postgres


@pytest.fixture(scope="module")
def seeded_database_url(postgres_container: PostgresContainer) -> str:
    database_url = postgres_container.get_connection_url(driver=None)
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SEED_SQL_PATH.read_text(encoding="utf-8"))
        connection.commit()
    return database_url


@pytest.fixture()
def restored_database_url(postgres_container: PostgresContainer) -> Iterator[str]:
    admin_url = _replace_database_name(
        postgres_container.get_connection_url(driver=None),
        "postgres",
    )
    database_name = f"restore_{uuid4().hex[:8]}"
    database_url = _create_database(admin_url, database_name)
    try:
        yield database_url
    finally:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)')


def test_dump_with_data_round_trips(
    seeded_database_url: str,
    restored_database_url: str,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "dump_fixture.sql"
    result = CliRunner().invoke(
        zbdump,
        [
            "--database_url",
            seeded_database_url,
            "--output_file",
            str(output_file),
            "--inserts_with_column_names",
            "dump_fixture",
        ],
    )

    assert result.exit_code == 0, result.output
    dump_text = output_file.read_text(encoding="utf-8")
    assert 'CREATE TABLE "dump_fixture"' in dump_text
    assert 'PRIMARY KEY ("id")' in dump_text
    assert dump_text.count('INSERT INTO "dump_fixture"') == 3
    assert "CREATE UNIQUE INDEX dump_fixture_external_uuid_idx" in dump_text

    _restore_dump(restored_database_url, output_file)

    with psycopg.connect(restored_database_url) as connection:
        with connection.cursor() as cursor:
            row_count = cursor.execute(
                "SELECT COUNT(*) FROM public.dump_fixture"
            ).fetchone()[0]
            index_names = {
                row[0]
                for row in cursor.execute(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename = 'dump_fixture'
                    """
                ).fetchall()
            }

    assert row_count == 3
    assert {
        "dump_fixture_pkey",
        "dump_fixture_external_uuid_idx",
        "dump_fixture_lower_email_idx",
        "dump_fixture_active_tenant_created_cover_idx",
        "dump_fixture_profile_gin_idx",
    } <= index_names


def test_dump_without_inserts_excludes_table_data(
    seeded_database_url: str,
    restored_database_url: str,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "dump_fixture_schema_only.sql"
    result = CliRunner().invoke(
        zbdump,
        [
            "--database_url",
            seeded_database_url,
            "--output_file",
            str(output_file),
            "dump_fixture",
        ],
    )

    assert result.exit_code == 0, result.output
    dump_text = output_file.read_text(encoding="utf-8")
    assert 'CREATE TABLE "dump_fixture"' in dump_text
    assert 'INSERT INTO "dump_fixture"' not in dump_text

    _restore_dump(restored_database_url, output_file)

    with psycopg.connect(restored_database_url) as connection:
        with connection.cursor() as cursor:
            row_count = cursor.execute(
                "SELECT COUNT(*) FROM public.dump_fixture"
            ).fetchone()[0]

    assert row_count == 0


def test_dump_missing_table_fails_with_clear_error(
    seeded_database_url: str,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "missing_table_dump.sql"
    result = CliRunner().invoke(
        zbdump,
        [
            "--database_url",
            seeded_database_url,
            "--output_file",
            str(output_file),
            "missing_table",
        ],
    )

    assert result.exit_code == 1
    assert 'Table "missing_table" does not exist.' in result.output
    assert not output_file.exists()
