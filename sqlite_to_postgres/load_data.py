from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from psycopg2 import errors

from unloading_from_sqlite import main


def load_from_sqlite(dsl: dict):
    """Основной метод загрузки данных из SQLite в Postgres."""
    main()
    create_tables(dsl)
    truncate_all_tables(dsl)
    find_path_all_csv_files(dsl)


def truncate_all_tables(dsl):
    with psycopg2.connect(**dsl) as pg_conn, pg_conn.cursor() as cursor:
        query = """DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (
                        SELECT tablename FROM pg_tables WHERE schemaname = current_schema()
                    )
                    LOOP
                        EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;"""
        cursor.execute(query)


def create_tables(dsl):
    with open(ddl_file_path) as file:
        ddl_file = ' '.join(line.strip() for line in file.readlines()).split('  ')

    with psycopg2.connect(**dsl) as pg_conn, pg_conn.cursor() as cursor:
        for command_sql in ddl_file:
            try:
                cursor.execute(command_sql)
            except (errors.DuplicateObject, errors.InFailedSqlTransaction):
                pass


def find_path_all_csv_files(dsl):
    path_all_csv_files = Path.cwd().rglob('*.csv')
    for csv_file_path in sorted(path_all_csv_files):
        fill_the_table(csv_file_path, dsl)


def fill_the_table(csv_file_path: Path, dsl: dict):
    table_name = Path(csv_file_path).parts[-1][:-4] # Delete suffix '.csv'

    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn, pg_conn.cursor() as cursor:
        query = f"""COPY {table_name} FROM STDIN DELIMITER ',' CSV HEADER"""
        try:
            cursor.copy_expert(query, open(csv_file_path, 'r'))
            cursor.execute(f"""SELECT COUNT(id) FROM {table_name}""")
            result = cursor.fetchone()
            print(f'Результат выполнения команды COPY {table_name} ', result)
        except (errors.UniqueViolation, errors.InFailedSqlTransaction):
            return


if __name__ == '__main__':
    ddl_file_path = Path(__file__).resolve().parent.parent.joinpath(
        'schema_design/movies_database.ddl')

    dsl = {
        'dbname': 'movies_database',
        'user': 'app',
        'password': '123qwe',
        'host': '127.0.0.1',
        'port': 5432,
        'options': '-c search_path=content',
    }
    load_from_sqlite(dsl)
