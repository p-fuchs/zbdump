FROM postgres:17

ENV POSTGRES_DB=zbdump_fixture
ENV POSTGRES_USER=zbdump
ENV POSTGRES_PASSWORD=zbdump

COPY docker/initdb/*.sql /docker-entrypoint-initdb.d/

EXPOSE 5432

HEALTHCHECK --interval=5s --timeout=5s --retries=10 CMD ["pg_isready", "-U", "zbdump", "-d", "zbdump_fixture"]
