import os
from pathlib import Path

import sqlite3
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

from schema import FilmWork, Genre, Person, GenreFilmWork, PersonFilmWork


load_dotenv()


def create_cursor_sqlite_and_postgres(
        connection: sqlite3.Connection, pg_conn: _connection
) -> tuple:
    pg_cursor = pg_conn.cursor()

    connection.row_factory = sqlite3.Row
    sqlite_cursor = connection.cursor()

    return sqlite_cursor, pg_cursor


def read_sqlite_tables_name(cursor: sqlite3.Cursor) -> list:
    cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    list_of_table_names = [data[0] for data in cursor.fetchall()]
    return list_of_table_names


def test_check_sqlite_postgres_consistency(
        sqlite_cursor: sqlite3.Cursor, pg_cursor: psycopg2.cursor
):
    list_of_table_names = read_sqlite_tables_name(sqlite_cursor)

    print('\tSQLite\t', 'Postgres')
    for table_name in list_of_table_names:
        query = f"""SELECT COUNT(id) FROM {table_name}"""

        sqlite_cursor.execute(query)
        result_sqlite = [i for i in sqlite_cursor.fetchone()]

        pg_cursor.execute(query)
        result_pg = pg_cursor.fetchone()

        print('Записей:',
              result_sqlite,
              '==',
              result_pg,
              f'{table_name}')

        assert result_sqlite == result_pg


def test_checking_the_contents_of_tables_entries(
        sqlite_cursor: sqlite3.Cursor, pg_cursor: psycopg2.cursor
):
    list_of_table_names = read_sqlite_tables_name(sqlite_cursor)

    for table_name in list_of_table_names:
        query = f"""SELECT id FROM {table_name}"""
        pg_cursor.execute(query)
        for pg_id in pg_cursor.fetchall():
            match table_name:
                case 'film_work':
                    pg_query = f"""SELECT
                                        id, title, description,
                                        creation_date, rating,
                                        film_type, created, modified
                                    FROM {table_name}
                                    WHERE id = '{pg_id[0]}'"""
                    pg_cursor.execute(pg_query)

                    pg_item = pg_cursor.fetchone()
                    pg_film_work = FilmWork(
                        id=pg_item[0],
                        title=pg_item[1],
                        description=pg_item[2],
                        creation_date=pg_item[3],
                        rating=pg_item[4],
                        film_type=pg_item[5],
                        created=str(pg_item[6])[:19],
                        modified=str(pg_item[7])[:19],
                    )

                    sqlite_query = f"""SELECT
                                            id, title, description,
                                            creation_date, rating,
                                            type, created_at, updated_at
                                        FROM {table_name}
                                        WHERE id = '{pg_id[0]}';"""
                    sqlite_cursor.execute(sqlite_query)

                    sqlite_item = sqlite_cursor.fetchone()
                    sqlite_film_work = FilmWork(
                        id=sqlite_item[0],
                        title=sqlite_item[1],
                        description=sqlite_item[2],
                        creation_date=sqlite_item[3],
                        rating=sqlite_item[4],
                        film_type=sqlite_item[5],
                        created=sqlite_item[6][:19],
                        modified=sqlite_item[7][:19],
                    )

                    assert pg_film_work == sqlite_film_work

                case 'genre_film_work':
                    pg_query = f"""SELECT
                                        id, film_work_id, genre_id, created
                                   FROM {table_name} WHERE id = '{pg_id[0]}'"""
                    pg_cursor.execute(pg_query)

                    pg_item = pg_cursor.fetchone()
                    pg_genre_film_work = GenreFilmWork(
                            id=pg_item[0],
                            film_work_id=pg_item[1],
                            genre_id=pg_item[2],
                            created=str(pg_item[3])[:19]
                    )

                    sqlite_query = f"""SELECT
                                            id, film_work_id, genre_id,
                                            created_at
                                       FROM {table_name}
                                       WHERE id = '{pg_id[0]}';"""
                    sqlite_cursor.execute(sqlite_query)

                    sqlite_item = sqlite_cursor.fetchone()
                    sqlite_genre_film_work = GenreFilmWork(
                            id=sqlite_item[0],
                            film_work_id=sqlite_item[1],
                            genre_id=sqlite_item[2],
                            created=sqlite_item[3][:19]
                    )

                    assert pg_genre_film_work == sqlite_genre_film_work

                case 'person_film_work':
                    pg_query = f"""SELECT
                                        id, film_work_id, person_id,
                                        role, created
                                   FROM {table_name} WHERE id = '{pg_id[0]}'"""
                    pg_cursor.execute(pg_query)

                    pg_item = pg_cursor.fetchone()
                    pg_person_film_work = PersonFilmWork(
                        id=pg_item[0],
                        film_work_id=pg_item[1],
                        person_id=pg_item[2],
                        role=pg_item[3],
                        created=str(pg_item[4])[:19]
                    )

                    sqlite_query = f"""SELECT
                                            id, film_work_id, person_id,
                                            role, created_at
                                       FROM {table_name}
                                       WHERE id = '{pg_id[0]}';"""
                    sqlite_cursor.execute(sqlite_query)

                    sqlite_item = sqlite_cursor.fetchone()
                    sqlite_person_film_work = PersonFilmWork(
                        id=sqlite_item[0],
                        film_work_id=sqlite_item[1],
                        person_id=sqlite_item[2],
                        role=sqlite_item[3],
                        created=sqlite_item[4][:19]
                    )

                    assert pg_person_film_work == sqlite_person_film_work

                case 'person':
                    pg_query = f"""SELECT * FROM {table_name}
                                    WHERE id = '{pg_id[0]}'"""
                    pg_cursor.execute(pg_query)

                    pg_item = pg_cursor.fetchone()
                    pg_person = Person(
                        id=pg_item[0],
                        full_name=pg_item[1],
                        created=str(pg_item[2])[:19],
                        modified=str(pg_item[3])[:19],
                    )

                    sqlite_query = f"""SELECT * FROM {table_name}
                                        WHERE id = '{pg_id[0]}';"""
                    sqlite_cursor.execute(sqlite_query)

                    sqlite_item = sqlite_cursor.fetchone()
                    sqlite_person = Person(
                        id=sqlite_item[0],
                        full_name=sqlite_item[1],
                        created=sqlite_item[2][:19],
                        modified=sqlite_item[3][:19],
                    )

                    assert pg_person == sqlite_person

                case 'genre':
                    pg_query = f"""SELECT * FROM {table_name}
                                    WHERE id = '{pg_id[0]}'"""
                    pg_cursor.execute(pg_query)

                    pg_item = pg_cursor.fetchone()
                    pg_genre = Genre(
                        id=pg_item[0],
                        name=pg_item[1],
                        description=pg_item[2],
                        created=str(pg_item[3])[:19],
                        modified=str(pg_item[4])[:19]
                    )

                    sqlite_query = f"""SELECT * FROM {table_name}
                                        WHERE id = '{pg_id[0]}';"""
                    sqlite_cursor.execute(sqlite_query)

                    sqlite_item = sqlite_cursor.fetchone()
                    sqlite_genre = Genre(
                        id=sqlite_item[0],
                        name=sqlite_item[1],
                        description=sqlite_item[2],
                        created=sqlite_item[3][:19],
                        modified=sqlite_item[4][:19]
                    )

                    assert pg_genre == sqlite_genre

                case _:
                    raise ValueError(f'Unknown table name: {table_name}')


def main(sqlite_conn: sqlite3.Connection, pg_conn: _connection):
    sqlite_cursor, pg_cursor = create_cursor_sqlite_and_postgres(
        sqlite_conn, pg_conn
    )
    test_check_sqlite_postgres_consistency(sqlite_cursor, pg_cursor)
    test_checking_the_contents_of_tables_entries(sqlite_cursor, pg_cursor)


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
        main(sqlite_conn, pg_conn)
