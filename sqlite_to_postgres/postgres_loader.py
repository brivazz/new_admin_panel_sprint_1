from dataclasses import astuple

from psycopg2.extras import execute_batch
from psycopg2 import errors
from psycopg2.extensions import connection as _connection

from settings import ddl_file_path


class PostgresSaver:
    def __init__(self, pg_cursor: _connection.cursor):
        self.cursor = pg_cursor

    def drop_all_schema_tables(self):
        query = """DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = current_schema()
                    )
                    LOOP
                        EXECUTE 'DROP TABLE '
                            || quote_ident(r.tablename) ||
                        ' CASCADE';
                    END LOOP;
                END $$;
                """
        self.cursor.execute(query)

    def create_tables(self):
        with open(ddl_file_path) as file:
            ddl_file = ' '.join(
                line.strip() for line in file.readlines()).split('  ')

        for command_sql in ddl_file:
            try:
                self.cursor.execute(command_sql)
            except (errors.DuplicateObject, errors.InFailedSqlTransaction):
                return

    def insert_data(self, table_name, extract_data: list, dataclass):
        args = [astuple(item) for item in extract_data]

        columns = ','.join(dataclass.__dict__['__match_args__'])
        tokens = ','.join(['%s'] * len(columns.split(',')))

        query = f"""
                INSERT INTO {table_name} ({columns}) VALUES ({tokens})"""
        try:
            execute_batch(self.cursor, query, args)
        except (errors.UniqueViolation, errors.InFailedSqlTransaction):
            return
