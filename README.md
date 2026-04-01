# zbdump

Tool that mimics MVP functionalities from the `pg_dump`. It allows to dump SQL table to the `.sql` file that recreates it.

## AI usage

To be entirely transparent, I've used AI to develop this script. The tests were entirely written by AI using my implementation plan with testcontainers.
80% of the script was written manually and it was only polished afterwards with the AI. The workflow was that AI tool generated sql that seeds the database
with pretty complex table `docker/initdb/001_fixture.sql` and then I was developing the script that was able to handle that. There were also issues that AI
review caught - for example I haven't really considered `AS IDENTITY` clauses which was pointed out by AI.

Surprisingly, AI roasted my implementation of being incomplete `pg_dump` and I needed to keep it away from implementing rest of the `pg_dump` features ;))

## Script usage

This project is based on the `uv` python package manager which is present on the students machine.
To run, this project you simply need to be in the project root and run the:

```bash
uv run zbdump.py
```

with any options you need. If you append `--help` you will see whole documentation:

```bash
$ uv run zbdump.py --help
Usage: zbdump.py [OPTIONS] TABLE

  Dump TABLE from the database.

Options:
  --database_url TEXT          Database connection URL, e.g.
                               postgresql://user:pass@host:5432/dbname. If not
                               provided, it will be inferred from the
                               DATABASE_URL environment variable  [required]
  --output_file PATH           Path to the file where the table dump should be
                               stored. Defaults to <table_name>_dump.sql in
                               the current working directory.
  --inserts_with_column_names  Include table data as INSERT statements with
                               explicit column names.
  --help                       Show this message and exit.
```

Example usage:

```bash
$ uv run zbdump.py \
    --database_url postgresql://zbdump:zbdump@localhost:5432/zbdump_fixture \
    --output_file dump_fixture.sql \
    --inserts_with_column_names \
    dump_fixture
```
