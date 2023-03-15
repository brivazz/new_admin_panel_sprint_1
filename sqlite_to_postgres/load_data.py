import os
import sys
import logging
import contextlib
from logging import StreamHandler, Formatter
from contextlib import contextmanager
from pathlib import Path
from typing import List, Any
from dataclasses import astuple

import sqlite3
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from psycopg2.extras import execute_batch
from psycopg2 import errors
from dotenv import load_dotenv

from schema import FilmWork, Genre, Person, GenreFilmWork, PersonFilmWork


load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = StreamHandler(stream=sys.stdout)
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

logger.debug('debug information')


datatables_list = {
    'film_work': FilmWork,
    'person': Person,
    'person_film_work': PersonFilmWork,
    'genre': Genre,
    'genre_film_work': GenreFilmWork,
    }


def load_from_sqlite(
        sqlite_cursor: sqlite3.Cursor, pg_cursor: _connection.cursor
):
    """Основной метод загрузки данных из SQLite в Postgres."""
    postgres_saver = PostgresSaver(pg_cursor)
    sqlite_extractor = SQLiteExtractor(sqlite_cursor, 1)

    drop_all_schema_tables(pg_cursor)
    postgres_saver.create_tables()

    for table_name, dataclass in datatables_list.items():
        try:
            extract_data = sqlite_extractor.format_dataclass_data(
                table_name, dataclass
            )
            postgres_saver.insert_data(table_name, extract_data, dataclass)
        except Exception as exception:
            logger.error(exception)


def reformat_sqlite_fields(elem: dict) -> dict:
    """
        Создаем функцию осуществления замены по различающимся полям данным.
        Список необходим для расширения, в случае добавления новых данных.
    """
    if 'created_at' in elem.keys():
        elem['created'] = elem['created_at']
        del (elem['created_at'])

    if 'updated_at' in elem.keys():
        elem['modified'] = elem['updated_at']
        del (elem['updated_at'])

    if 'type' in elem.keys():
        elem['film_type'] = elem['type']
        del (elem['type'])

    if 'file_path' in elem.keys():
        del (elem['file_path'])

    return elem


def _prepare_data(sqlite_cursor: sqlite3.Cursor, row: list) -> dict:
    data = {}
    for index, column in enumerate(sqlite_cursor.description):
        data[column[0]] = row[index]

    return data


class SQLiteExtractor:
    def __init__(self, sqlite_cursor: sqlite3.Cursor, package_limit: int):
        self.cursor = sqlite_cursor
        self.package_limit = package_limit

    def load_sqlite(self, table: str) -> tuple:
        try:
            self.cursor.row_factory = _prepare_data
            try:
                self.cursor.execute(f'SELECT * FROM {table}')
            except sqlite3.Error as e:
                raise e

            while True:
                rows = self.cursor.fetchmany(size=self.package_limit)

                if not rows:
                    return

                yield from rows
        except Exception as exception:
            logger.error(exception)

    def format_dataclass_data(self, table_name: str, dataclass) -> List[Any]:
        data = self.load_sqlite(table_name)
        return [dataclass(**reformat_sqlite_fields(elem)) for elem in data]


def drop_all_schema_tables(pg_cursor: _connection.cursor):
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
    pg_cursor.execute(query)


class PostgresSaver:
    def __init__(self, pg_cursor: _connection.cursor):
        self.cursor = pg_cursor

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

        column = ','.join(dataclass.__dict__['__match_args__'])
        tokens = ','.join(['%s'] * len(dataclass.__dict__['__match_args__']))

        query = f"""
                INSERT INTO {table_name} ({column}) VALUES ({tokens})"""
        try:
            execute_batch(self.cursor, query, args)
        except (errors.UniqueViolation, errors.InFailedSqlTransaction):
            return


@contextmanager
def open_sqlite_db(db_sqlite_path: Path):
    connection = sqlite3.connect(db_sqlite_path)
    try:
        yield connection.cursor()
    finally:
        connection.commit()
        connection.close()


if __name__ == '__main__':
    BASE_DIR = Path(__file__).resolve().parent
    db_sqlite_path = Path.joinpath(BASE_DIR, 'db.sqlite')
    ddl_file_path = Path(__file__).resolve().parent.parent.joinpath(
        'schema_design/movies_database.ddl'
    )

    dsn = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'port': os.environ.get('DB_PORT', 5432),
        'options': '-c search_path=content',
    }

    with open_sqlite_db(db_sqlite_path) as sqlite_cursor, contextlib.closing(
        psycopg2.connect(**dsn, cursor_factory=DictCursor)
    ) as pg_conn, pg_conn.cursor() as pg_cursor:
        load_from_sqlite(sqlite_cursor, pg_cursor)
        pg_conn.commit()
