import csv
import sqlite3
from pathlib import Path

from schema import FilmWork, Genre, Person, GenreFilmWork, PersonFilmWork


BASE_DIR = Path(__file__).resolve().parent
db_path = Path.joinpath(BASE_DIR, 'db.sqlite')


def read_sqlite_tables_name(connection: sqlite3.Connection) -> tuple:
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type = \"table\"")
    list_of_table_names = [data[0] for data in cursor.fetchall()]
    return list_of_table_names, cursor


def extract_tables(list_of_table_names: list, cursor: sqlite3.Cursor):
    for table_name in list_of_table_names:
        result = []

        if table_name == 'genre_film_work':
            table_columns = ['id', 'film_work_id', 'genre_id', 'created']
            query = f"""SELECT
                        id, film_work_id, genre_id, created_at
                        FROM {table_name};"""
            cursor.execute(query)
            items = cursor.fetchall()
            for item in items:
                genre_film_work = GenreFilmWork(
                    id=item[0],
                    film_work_id=item[1],
                    genre_id=item[2],
                    created=item[3]
                )
                result.append(genre_film_work)
            write_to_csv(result, table_name, table_columns)

        elif table_name == 'person_film_work':
            table_columns = [
                'id', 'film_work_id', 'person_id', 'role', 'created'
            ]
            query = f"""SELECT
                            id, film_work_id, person_id,
                            role, created_at
                            FROM {table_name};"""
            cursor.execute(query)
            items = cursor.fetchall()
            for item in items:
                person_film_work = PersonFilmWork(
                    id=item[0],
                    film_work_id=item[1],
                    person_id=item[2],
                    role=item[3],
                    created=item[4]
                )
                result.append(person_film_work)
            write_to_csv(result, table_name, table_columns)

        elif table_name == 'person':
            table_columns = ['id', 'full_name', 'created', 'modified']
            query = f"""SELECT * FROM {table_name};"""
            cursor.execute(query)
            items = cursor.fetchall()
            for item in items:
                person = Person(
                    id=item[0],
                    full_name=item[1],
                    created=item[2],
                    modified=item[3],
                )
                result.append(person)
            write_to_csv(result, table_name, table_columns)

        elif table_name == 'film_work':
            table_columns = [
                'id', 'title', 'description', 'creation_date', 'rating',
                'film_type', 'created', 'modified'
            ]
            query = f"""SELECT
                        id, title, description,
                        creation_date, rating,
                        type, created_at, updated_at 
                        FROM {table_name};"""
            cursor.execute(query)
            items = cursor.fetchall()
            for item in items:
                film_work = FilmWork(
                    id=item[0],
                    title=item[1],
                    description=item[2],
                    creation_date=item[3],
                    rating=item[4],
                    film_type=item[5],
                    created=item[6],
                    modified=item[7],
                )
                result.append(film_work)
            write_to_csv(result, table_name, table_columns)

        elif table_name == 'genre':
            table_columns = ['id', 'name', 'description', 'created', 'modified']
            query = f"""SELECT * FROM {table_name};"""
            cursor.execute(query)
            items = cursor.fetchall()
            for item in items:
                genre = Genre(
                    id=item[0],
                    name=item[1],
                    description=item[2],
                    created=item[3],
                    modified=item[4]
                )
                result.append(genre)
            write_to_csv(result, table_name, table_columns)

    cursor.close()


def write_to_csv(result: list, table_name: str, table_columns: list):
    with open(f'{BASE_DIR/table_name}.csv', 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(table_columns)

        for i in result:
            if table_name == 'person_film_work':
                writer.writerow(
                    (
                        i.id, i.film_work_id, i.person_id,
                        i.role, i.created
                    ))
            elif table_name == 'person':
                writer.writerow(
                    (
                        i.id, i.full_name, i.created, i.modified
                    ))
            elif table_name == 'film_work':
                writer.writerow(
                    (
                        i.id, i.title, i.description,
                        i.creation_date, i.rating,
                        i.film_type, i.created, i.modified
                    ))
            elif table_name == 'genre_film_work':
                writer.writerow(
                    (
                        i.id, i.film_work_id, i.genre_id, i.created
                    ))
            elif table_name == 'genre':
                writer.writerow(
                    (
                        i.id, i.name, i.description,
                        i.created, i.modified
                    ))


def main():
    with sqlite3.connect(db_path) as connection:
        list_of_table_names, cursor = read_sqlite_tables_name(connection)
        extract_tables(list_of_table_names, cursor)
