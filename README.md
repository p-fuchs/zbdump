# zbdump fixture database

This repository now includes a PostgreSQL 17 fixture database for manual `zbdump` testing.
It does not change the existing Python dumper. The fixture gives you one table,
seed data, and several index shapes, including a covering index with `INCLUDE`.

## What gets created

The database contains one table: `public.dump_fixture`.

It is designed to be useful for dump testing:

- primary key and identity column
- nullable and non-nullable columns
- scalar types such as `integer`, `bigint`, `numeric`, `boolean`, `uuid`
- temporal types such as `date`, `time`, `timestamp`, `timestamptz`, `interval`
- richer types such as `jsonb`, `text[]`, `bytea`, `inet`
- multiple index types:
  - unique btree index
  - expression index
  - partial covering index with `INCLUDE`
  - GIN index on `jsonb`

## Run with Docker Compose

Start the database:

```bash
docker compose up -d
```

Stop it:

```bash
docker compose down
```

Reset the database and rerun the seed scripts:

```bash
docker compose down -v
docker compose up -d
```

View logs:

```bash
docker compose logs -f postgres
```

## Connection details

- host: `localhost`
- port: `5432`
- database: `zbdump_fixture`
- user: `zbdump`
- password: `zbdump`

Connection string:

```text
postgresql://zbdump:zbdump@localhost:5432/zbdump_fixture
```

## Verify the fixture

Inspect the table:

```bash
docker compose exec postgres psql -U zbdump -d zbdump_fixture -c '\d+ public.dump_fixture'
```

Check the row count:

```bash
docker compose exec postgres psql -U zbdump -d zbdump_fixture -c 'SELECT COUNT(*) FROM public.dump_fixture;'
```

List indexes:

```bash
docker compose exec postgres psql -U zbdump -d zbdump_fixture -c "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'dump_fixture' ORDER BY indexname;"
```

Show the seeded rows:

```bash
docker compose exec postgres psql -U zbdump -d zbdump_fixture -c 'SELECT id, tenant_id, external_uuid, email, display_name FROM public.dump_fixture ORDER BY id;'
```

## Example dump commands

Generate schema and indexes only:

```bash
source .venv/bin/activate
python zbdump.py --database_url postgresql://zbdump:zbdump@localhost:5432/zbdump_fixture dump_fixture
```

Generate schema, data `INSERT`s with explicit column names, and indexes:

```bash
source .venv/bin/activate
python zbdump.py --database_url postgresql://zbdump:zbdump@localhost:5432/zbdump_fixture --inserts_with_column_names dump_fixture
```

## Run integration tests

The integration suite starts PostgreSQL 17 with `testcontainers`, seeds the fixture schema,
executes the dumper, and restores the generated SQL into a fresh database.

```bash
source .venv/bin/activate
PYTHONPATH=$PWD uv run pytest tests/test_integration.py
```

## Notes

- The SQL files in `/docker-entrypoint-initdb.d/` run only when PostgreSQL initializes a fresh data directory.
- Because Compose uses a named volume, normal restarts keep the same data.
- If you want the initial seed SQL to run again, use `docker compose down -v`.
- If you test the dumper, the table name to dump is `dump_fixture`.
