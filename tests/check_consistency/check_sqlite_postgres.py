import os
from pathlib import Path

import sqlite3
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv


load_dotenv()


def check_consistency_sqlite_postgres(
        connection: sqlite3.Connection, pg_conn: _connection
):
    pg_cursor = pg_conn.cursor()

    connection.row_factory = sqlite3.Row
    sqlite_sursor = connection.cursor()

    sqlite_sursor.execute("SELECT name FROM sqlite_master WHERE type = \"table\"")
    list_of_table_names = [data[0] for data in sqlite_sursor.fetchall()]

    for table_name in list_of_table_names:
        sqlite_sursor.execute(f"""SELECT COUNT(id) FROM {table_name}""")
        result_sqlite = sqlite_sursor.fetchone()
        print(f'SQLite в таблице: {table_name} ', [i for i in result_sqlite], 'записей.')

        pg_cursor.execute(f"""SELECT COUNT(id) FROM {table_name}""")
        result_pg = pg_cursor.fetchone()
        print(f'Postgres в таблице: {table_name} ', result_pg, 'записей.')
        print()

        result_sqlite = [i for i in result_sqlite]

        assert result_sqlite == result_pg


if __name__ == '__main__':
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    db_sqlite_path = Path.joinpath(BASE_DIR, 'sqlite_to_postgres/db.sqlite')

    dsl = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST', '127.0.0.1'),
        'port': os.environ.get('DB_PORT', 5432),
        'options': '-c search_path=content',
    }

    with sqlite3.connect(db_sqlite_path) as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        check_consistency_sqlite_postgres(sqlite_conn, pg_conn)
