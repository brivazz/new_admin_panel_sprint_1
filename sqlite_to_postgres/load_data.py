import contextlib
import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from postgres_loader import PostgresSaver
from sqlite_loader import SQLiteExtractor, open_sqlite_db
from schema import datatables_list
from settings import logger, dsn, db_sqlite_path


def load_from_sqlite(
        sqlite_cursor: sqlite3.Cursor, pg_cursor: _connection.cursor
):
    """Основной метод загрузки данных из SQLite в Postgres."""
    postgres_saver = PostgresSaver(pg_cursor)
    sqlite_extractor = SQLiteExtractor(sqlite_cursor, 1)

    postgres_saver.drop_all_schema_tables()
    postgres_saver.create_tables()

    for table_name, dataclass in datatables_list.items():
        try:
            extract_data = sqlite_extractor.format_dataclass_data(
                table_name, dataclass
            )
            postgres_saver.insert_data(table_name, extract_data, dataclass)
        except Exception as exception:
            logger.error(exception)


if __name__ == '__main__':
    with open_sqlite_db(db_sqlite_path) as sqlite_cursor, contextlib.closing(
        psycopg2.connect(**dsn, cursor_factory=DictCursor)
    ) as pg_conn, pg_conn.cursor() as pg_cursor:
        load_from_sqlite(sqlite_cursor, pg_cursor)
        pg_conn.commit()
