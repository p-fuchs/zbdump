from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from click.testing import CliRunner
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from zbdump import DatabaseConnectionProtocol, zbdump

SEED_SQL_PATH = (
    Path(__file__).resolve().parents[1] / "docker" / "initdb" / "001_fixture.sql"
)


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


def _seed_database(database_url: str) -> None:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(SEED_SQL_PATH.read_text(encoding="utf-8"))
        connection.commit()


def _count_rows(database_url: str) -> int:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            row = cursor.execute("SELECT COUNT(*) FROM public.dump_fixture").fetchone()
            assert row is not None
            return row[0]


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
    _seed_database(database_url)
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
                cursor.execute(
                    f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)'
                )


@pytest.fixture()
def source_database_url(postgres_container: PostgresContainer) -> Iterator[str]:
    admin_url = _replace_database_name(
        postgres_container.get_connection_url(driver=None),
        "postgres",
    )
    database_name = f"source_{uuid4().hex[:8]}"
    database_url = _create_database(admin_url, database_name)
    _seed_database(database_url)
    try:
        yield database_url
    finally:
        with psycopg.connect(admin_url, autocommit=True) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f'DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)'
                )


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
    assert 'INSERT INTO "dump_fixture" ("id", "tenant_id"' in dump_text
    assert "CREATE UNIQUE INDEX dump_fixture_external_uuid_idx" in dump_text

    _restore_dump(restored_database_url, output_file)

    with psycopg.connect(restored_database_url) as connection:
        with connection.cursor() as cursor:
            row = cursor.execute("SELECT COUNT(*) FROM public.dump_fixture").fetchone()
            assert row is not None
            row_count = row[0]
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


def test_dump_without_column_names_still_includes_table_data(
    seeded_database_url: str,
    restored_database_url: str,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "dump_fixture_without_column_names.sql"
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
    assert dump_text.count('INSERT INTO "dump_fixture"') == 3
    assert 'INSERT INTO "dump_fixture" OVERRIDING SYSTEM VALUE VALUES' in dump_text
    assert 'INSERT INTO "dump_fixture" ("id", "tenant_id"' not in dump_text

    _restore_dump(restored_database_url, output_file)

    with psycopg.connect(restored_database_url) as connection:
        with connection.cursor() as cursor:
            row = cursor.execute("SELECT COUNT(*) FROM public.dump_fixture").fetchone()
            assert row is not None
            row_count = row[0]

    assert row_count == 3


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


def test_dump_uses_current_schema_when_same_table_name_exists_elsewhere(
    seeded_database_url: str,
    restored_database_url: str,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "dump_fixture_current_schema.sql"

    with psycopg.connect(seeded_database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE SCHEMA shadow")
            cursor.execute(
                """
                CREATE TABLE shadow.dump_fixture (
                    id text PRIMARY KEY,
                    shadow_only text NOT NULL
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO shadow.dump_fixture (id, shadow_only)
                VALUES ('shadow-id', 'shadow-value')
                """
            )
        connection.commit()

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
    assert '"shadow_only"' not in dump_text
    assert '"id" bigint GENERATED ALWAYS AS IDENTITY NOT NULL' in dump_text
    assert 'INSERT INTO "dump_fixture" OVERRIDING SYSTEM VALUE VALUES' in dump_text

    _restore_dump(restored_database_url, output_file)

    with psycopg.connect(restored_database_url) as connection:
        with connection.cursor() as cursor:
            restored_columns = {
                row[0]
                for row in cursor.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'dump_fixture'
                    """
                ).fetchall()
            }
            row_count = cursor.execute(
                "SELECT COUNT(*) FROM public.dump_fixture"
            ).fetchone()
            assert row_count is not None
            row_count = row_count[0]

    assert "shadow_only" not in restored_columns
    assert row_count == 3


def test_dump_with_more_than_fifty_batches_of_rows_round_trips(
    source_database_url: str,
    restored_database_url: str,
    tmp_path: Path,
) -> None:
    extra_row_count = DatabaseConnectionProtocol.ROW_FETCH_BATCH_SIZE * 50
    output_file = tmp_path / "dump_fixture_large.sql"

    with psycopg.connect(source_database_url) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO public.dump_fixture (
                    tenant_id,
                    external_uuid,
                    email,
                    display_name,
                    is_active
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    (
                        1000 + index,
                        uuid4(),
                        f"bulk-{index}@example.com",
                        f"Bulk Row {index}",
                        index % 2 == 0,
                    )
                    for index in range(1, extra_row_count + 1)
                ),
            )
        connection.commit()

    expected_row_count = _count_rows(source_database_url)
    assert expected_row_count == extra_row_count + 3

    result = CliRunner().invoke(
        zbdump,
        [
            "--database_url",
            source_database_url,
            "--output_file",
            str(output_file),
            "--inserts_with_column_names",
            "dump_fixture",
        ],
    )

    assert result.exit_code == 0, result.output

    _restore_dump(restored_database_url, output_file)

    restored_row_count = _count_rows(restored_database_url)
    assert restored_row_count == expected_row_count


def test_dump_simple_table_with_column_names_without_identity_generation(
    source_database_url: str,
    restored_database_url: str,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "simple_fixture.sql"

    with psycopg.connect(source_database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE public.simple_fixture (
                    id integer PRIMARY KEY,
                    name text NOT NULL,
                    is_active boolean NOT NULL DEFAULT true
                )
                """
            )
            cursor.execute(
                """
                CREATE INDEX simple_fixture_name_idx
                ON public.simple_fixture (name)
                """
            )
            cursor.execute(
                """
                INSERT INTO public.simple_fixture (id, name, is_active)
                VALUES
                    (1, 'Alice', true),
                    (2, 'Bob', false)
                """
            )
        connection.commit()

    result = CliRunner().invoke(
        zbdump,
        [
            "--database_url",
            source_database_url,
            "--output_file",
            str(output_file),
            "--inserts_with_column_names",
            "simple_fixture",
        ],
    )

    assert result.exit_code == 0, result.output
    dump_text = output_file.read_text(encoding="utf-8")
    assert 'CREATE TABLE "simple_fixture"' in dump_text
    assert 'PRIMARY KEY ("id")' in dump_text
    assert "CREATE INDEX simple_fixture_name_idx" in dump_text
    assert (
        'INSERT INTO "simple_fixture" ("id", "name", "is_active") VALUES' in dump_text
    )
    assert "OVERRIDING SYSTEM VALUE" not in dump_text

    _restore_dump(restored_database_url, output_file)

    with psycopg.connect(restored_database_url) as connection:
        with connection.cursor() as cursor:
            row = cursor.execute(
                "SELECT COUNT(*) FROM public.simple_fixture"
            ).fetchone()
            assert row is not None
            row_count = row[0]
            index_names = {
                row[0]
                for row in cursor.execute(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename = 'simple_fixture'
                    """
                ).fetchall()
            }

    assert row_count == 2
    assert {"simple_fixture_pkey", "simple_fixture_name_idx"} <= index_names


def test_dump_skips_custom_named_primary_key_backing_index(
    source_database_url: str,
    restored_database_url: str,
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "custom_named_pk_fixture.sql"

    with psycopg.connect(source_database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE public.custom_named_pk_fixture (
                    id integer NOT NULL,
                    payload text NOT NULL,
                    CONSTRAINT custom_named_pk PRIMARY KEY (id)
                )
                """
            )
            cursor.execute(
                """
                INSERT INTO public.custom_named_pk_fixture (id, payload)
                VALUES (1, 'alpha'), (2, 'beta')
                """
            )
        connection.commit()

    result = CliRunner().invoke(
        zbdump,
        [
            "--database_url",
            source_database_url,
            "--output_file",
            str(output_file),
            "custom_named_pk_fixture",
        ],
    )

    assert result.exit_code == 0, result.output

    _restore_dump(restored_database_url, output_file)

    with psycopg.connect(restored_database_url) as connection:
        with connection.cursor() as cursor:
            index_names = {
                row[0]
                for row in cursor.execute(
                    """
                    SELECT indexname
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND tablename = 'custom_named_pk_fixture'
                    """
                ).fetchall()
            }

    assert index_names == {"custom_named_pk_fixture_pkey"}
