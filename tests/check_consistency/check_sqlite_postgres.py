import os
from pathlib import Path

import sqlite3
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv


load_dotenv()


def read_sqlite_tables_name(cursor: sqlite3.Cursor) -> tuple:
    cursor.execute("SELECT name FROM sqlite_master WHERE type = \"table\"")
    list_of_table_names = [data[0] for data in cursor.fetchall()]
    return list_of_table_names


def test_check_sqlite_postgres_consistency(
        connection: sqlite3.Connection, pg_conn: _connection
):
    pg_cursor = pg_conn.cursor()
    connection.row_factory = sqlite3.Row
    sqlite_cursor = connection.cursor()
    list_of_table_names = read_sqlite_tables_name(sqlite_cursor)

    print('\tSQLite\t', 'Postgres')
    for table_name in list_of_table_names:
        sqlite_cursor.execute(f"""SELECT COUNT(id) FROM {table_name}""")
        result_sqlite = sqlite_cursor.fetchone()

        pg_cursor.execute(f"""SELECT COUNT(id) FROM {table_name}""")
        result_pg = pg_cursor.fetchone()
        print('Записей:',
              [i for i in result_sqlite],
              '==',
              result_pg,
              f'{table_name}'
        )
        result_sqlite = [i for i in result_sqlite]
        assert result_sqlite == result_pg


def test_checking_the_contents_of_table_entries(
        connection: sqlite3.Connection, pg_conn: _connection
):
    pg_cursor = pg_conn.cursor()
    connection.row_factory = sqlite3.Row
    sqlite_cursor = connection.cursor()
    list_of_table_names = read_sqlite_tables_name(sqlite_cursor)

    for table_name in list_of_table_names:
        match table_name:
            case 'film_work':
                query = f"""SELECT
                                id, title, description,
                                creation_date, rating,
                                type, created_at, updated_at
                            FROM {table_name} where id = '3d825f60-9fff-4dfe-b294-1a45fa1e115d';"""
                sqlite_cursor.execute(query)
                # items_sqlite = sqlite_cursor.fetchone()
                items_sqlite = [i for i in sqlite_cursor.fetchone()]
                # print([i for i in sqlite_cursor.fetchone()])
                print(items_sqlite)

                query = f"""SELECT
                                id, title, description,
                                creation_date, rating,
                                film_type, created, modified
                            FROM {table_name}"""
                pg_cursor.execute(query)
                pg_items = pg_cursor.fetchone()
                print(pg_items)
                # assert items_sqlite == pg_items


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
        # test_check_sqlite_postgres_consistency(sqlite_conn, pg_conn)
        test_checking_the_contents_of_table_entries(sqlite_conn, pg_conn)
