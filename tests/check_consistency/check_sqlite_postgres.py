from pathlib import Path

import sqlite3
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor


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
        print(f'Количество записей SQLite в таблице: {table_name} ', [i for i in result_sqlite], 'штук.')

        pg_cursor.execute(f"""SELECT COUNT(id) FROM {table_name}""")
        result_pg = pg_cursor.fetchone()
        print(f'Количество записей Postgres в таблице: {table_name} ', result_pg, 'штук.')
        print()

        result_sqlite = [i for i in result_sqlite]

        assert result_sqlite == result_pg



if __name__ == '__main__':
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    db_sqlite_path = Path.joinpath(BASE_DIR, 'sqlite_to_postgres/db.sqlite')

    dsl = {
        'dbname': 'movies_database',
        'user': 'app',
        'password': '123qwe',
        'host': '127.0.0.1',
        'port': 5432,
        'options': '-c search_path=content',
    }

    with sqlite3.connect(db_sqlite_path) as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        check_consistency_sqlite_postgres(sqlite_conn, pg_conn)
