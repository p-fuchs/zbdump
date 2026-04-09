from collections.abc import Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, TextIO

import click
import psycopg
from psycopg import sql
from psycopg.types.json import Json, Jsonb


@dataclass
class TableColumnInfo:
    ordinal_position: int
    column_name: str
    rendered_data_type: str
    is_nullable: bool
    column_default: str | None
    is_identity: bool
    identity_generation: str | None


@dataclass
class TableIndexInfo:
    index_name: str
    index_def: str
    is_primary_key_backing: bool = False


@dataclass
class TableInfo:
    table_name: str
    columns: list[TableColumnInfo]
    indices: list[TableIndexInfo]
    primary_key_columns: list[str]


class TableNotFoundError(Exception):
    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        super().__init__(f'Table "{table_name}" does not exist.')


class DatabaseConnectionProtocol:
    ROW_FETCH_BATCH_SIZE = 1000

    def __init__(self, connection: psycopg.Connection) -> None:
        self._connection = connection

    def _read_table_index_data(
        self, table_name: str, cursor: psycopg.Cursor
    ) -> list[TableIndexInfo]:
        result = cursor.execute(
            """
            SELECT
                idx.relname AS indexname,
                pg_get_indexdef(i.indexrelid) AS indexdef,
                COALESCE(con.contype = 'p', false) AS is_primary_key_backing
            FROM pg_catalog.pg_class tbl
            JOIN pg_catalog.pg_namespace ns
                ON ns.oid = tbl.relnamespace
            JOIN pg_catalog.pg_index i
                ON i.indrelid = tbl.oid
            JOIN pg_catalog.pg_class idx
                ON idx.oid = i.indexrelid
            LEFT JOIN pg_catalog.pg_constraint con
                ON con.conindid = i.indexrelid
                AND con.contype = 'p'
            WHERE tbl.relname = %s
              AND ns.nspname = current_schema()
            ORDER BY idx.relname;
            """,
            (table_name,),
        ).fetchall()

        return [
            TableIndexInfo(
                index_name=row[0],
                index_def=row[1],
                is_primary_key_backing=row[2],
            )
            for row in result
        ]

    def _read_table_columns_data(
        self, table_name: str, cursor: psycopg.Cursor
    ) -> list[TableColumnInfo]:
        result = cursor.execute(
            """
            SELECT
                c.ordinal_position,
                c.column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS rendered_data_type,
                c.is_nullable,
                c.column_default,
                c.is_identity,
                c.identity_generation
            FROM information_schema.columns c
            JOIN pg_catalog.pg_class cls
                ON cls.relname = c.table_name
            JOIN pg_catalog.pg_namespace ns
                ON ns.oid = cls.relnamespace
                AND ns.nspname = c.table_schema
            JOIN pg_catalog.pg_attribute a
                ON a.attrelid = cls.oid
                AND a.attname = c.column_name
            WHERE c.table_name = %s
              AND c.table_schema = current_schema()
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY c.ordinal_position;
            """,
            (table_name,),
        ).fetchall()

        return [
            TableColumnInfo(
                ordinal_position=row[0],
                column_name=row[1],
                rendered_data_type=row[2],
                is_nullable=row[3].lower() == "yes",
                column_default=row[4],
                is_identity=row[5].lower() == "yes",
                identity_generation=row[6],
            )
            for row in result
        ]

    def _read_primary_key_columns(
        self, table_name: str, cursor: psycopg.Cursor
    ) -> list[str]:
        result = cursor.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.table_name = %s
              AND tc.table_schema = current_schema()
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
            """,
            (table_name,),
        ).fetchall()

        return [row[0] for row in result]

    def read_table_config(self, table_name: str) -> TableInfo:
        with self._connection.cursor() as cur:
            columns = self._read_table_columns_data(table_name, cur)
            if not columns:
                raise TableNotFoundError(table_name)

            return TableInfo(
                table_name=table_name,
                columns=columns,
                indices=self._read_table_index_data(table_name, cur),
                primary_key_columns=self._read_primary_key_columns(table_name, cur),
            )

    def iter_table_rows(
        self, table_info: TableInfo
    ) -> Generator[tuple[str, ...], None, None]:
        selected_columns = sql.SQL(", ").join(
            sql.Identifier(column.column_name) for column in table_info.columns
        )
        query = sql.SQL("SELECT {columns} FROM {table_name}").format(
            columns=selected_columns,
            table_name=sql.Identifier(table_info.table_name),
        )

        if table_info.primary_key_columns:
            ordered_primary_key_columns = sql.SQL(", ").join(
                sql.Identifier(column_name)
                for column_name in table_info.primary_key_columns
            )
            query = query + sql.SQL(" ORDER BY {columns}").format(
                columns=ordered_primary_key_columns
            )

        with self._connection.cursor(
            name=f"{table_info.table_name}_rows_cursor"
        ) as cur:
            cur.execute(query)

            while True:
                rows = cur.fetchmany(self.ROW_FETCH_BATCH_SIZE)
                if not rows:
                    break

                for row in rows:
                    yield tuple(
                        self._render_sql_literal(column, value)
                        for column, value in zip(table_info.columns, row, strict=True)
                    )

    def _render_sql_literal(self, column: TableColumnInfo, value: Any) -> str:
        if value is None:
            return sql.Literal(value).as_string(self._connection)
        if column.rendered_data_type == "json":
            return sql.Literal(Json(value)).as_string(self._connection)
        if column.rendered_data_type == "jsonb":
            return sql.Literal(Jsonb(value)).as_string(self._connection)
        return sql.Literal(value).as_string(self._connection)


class DumpFileRenderer:
    def __init__(self, file: TextIO) -> None:
        self._file = file

    def render_table_ddl(self, table_info: TableInfo) -> None:
        column_lines = [
            self._render_column_definition(column) for column in table_info.columns
        ]

        if table_info.primary_key_columns:
            primary_key_columns = ", ".join(
                self._quote_identifier(column_name)
                for column_name in table_info.primary_key_columns
            )
            column_lines.append(f"PRIMARY KEY ({primary_key_columns})")

        rendered_columns = ",\n".join(f"    {line}" for line in column_lines)
        self._file.write(
            f"CREATE TABLE {self._quote_identifier(table_info.table_name)} (\n"
            f"{rendered_columns}\n"
            ");\n\n"
        )

    def render_table_data(
        self,
        table_info: TableInfo,
        rows: Iterable[tuple[str, ...]],
        *,
        include_column_names: bool,
    ) -> None:
        rendered_column_names = ", ".join(
            self._quote_identifier(column.column_name) for column in table_info.columns
        )
        column_list = f" ({rendered_column_names})" if include_column_names else ""
        overriding_clause = self._render_identity_override_clause(table_info)
        wrote_any_rows = False

        for row in rows:
            rendered_values = ", ".join(row)
            self._file.write(
                f"INSERT INTO {self._quote_identifier(table_info.table_name)}"
                f"{column_list}{overriding_clause} VALUES ({rendered_values});\n"
            )
            wrote_any_rows = True

        if wrote_any_rows:
            self._file.write("\n")

    def render_table_indexes(self, table_info: TableInfo) -> None:
        index_definitions = [
            self._terminate_sql_statement(index.index_def)
            for index in table_info.indices
            if not index.is_primary_key_backing
        ]

        if not index_definitions:
            return

        self._file.write("\n".join(index_definitions))
        self._file.write("\n")

    def _render_column_definition(self, column: TableColumnInfo) -> str:
        column_parts = [
            self._quote_identifier(column.column_name),
            column.rendered_data_type,
        ]

        if column.is_identity:
            identity_generation = column.identity_generation or "BY DEFAULT"
            column_parts.append(f"GENERATED {identity_generation} AS IDENTITY")
        elif column.column_default is not None:
            column_parts.append(f"DEFAULT {column.column_default}")

        if not column.is_nullable:
            column_parts.append("NOT NULL")

        return " ".join(column_parts)

    def _quote_identifier(self, identifier: str) -> str:
        escaped_identifier = identifier.replace('"', '""')
        return f'"{escaped_identifier}"'

    def _render_identity_override_clause(self, table_info: TableInfo) -> str:
        has_generated_always_identity = any(
            column.is_identity and column.identity_generation == "ALWAYS"
            for column in table_info.columns
        )
        if has_generated_always_identity:
            return " OVERRIDING SYSTEM VALUE"
        return ""

    def _terminate_sql_statement(self, statement: str) -> str:
        stripped_statement = statement.rstrip()
        if stripped_statement.endswith(";"):
            return stripped_statement
        return f"{stripped_statement};"

    @classmethod
    def from_path(cls, path: Path) -> "DumpFileRenderer":
        file = open(path, "w", encoding="utf-8")
        return cls(file)

    def __enter__(self) -> "DumpFileRenderer":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._file.close()


@contextmanager
def get_database_connection(
    database_url: str,
) -> Generator[psycopg.Connection, None, None]:
    with psycopg.connect(database_url) as connection:
        connection.read_only = True
        yield connection


@click.command()
@click.option(
    "--database_url",
    envvar="DATABASE_URL",
    required=True,
    type=click.STRING,
    help="Database connection URL, e.g. postgresql://user:pass@host:5432/dbname. If not provided, it will be inferred from the DATABASE_URL environment variable",
)
@click.option(
    "--output_file",
    required=False,
    type=click.Path(path_type=Path),
    help="Path to the file where the table dump should be stored. Defaults to <table_name>_dump.sql in the current working directory.",
)
@click.option(
    "--inserts_with_column_names",
    is_flag=True,
    help="Include table data as INSERT statements with explicit column names.",
)
@click.argument(
    "table",
    required=True,
    type=click.STRING,
)
def zbdump(
    database_url: str,
    table: str,
    output_file: Path | None,
    inserts_with_column_names: bool,
):
    """Dump TABLE from the database."""
    if output_file is None:
        output_file = Path(f"{table}_dump.sql").absolute()

    with get_database_connection(database_url) as connection:
        connection_protocol = DatabaseConnectionProtocol(connection)
        try:
            table_info = connection_protocol.read_table_config(table)
        except TableNotFoundError as exc:
            raise click.ClickException(str(exc)) from exc

        with DumpFileRenderer.from_path(output_file) as renderer:
            renderer.render_table_ddl(table_info)
            table_rows = connection_protocol.iter_table_rows(table_info)
            renderer.render_table_data(
                table_info,
                table_rows,
                include_column_names=inserts_with_column_names,
            )
            renderer.render_table_indexes(table_info)


if __name__ == "__main__":
    zbdump()
