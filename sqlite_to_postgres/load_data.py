import csv
from pathlib import Path

import sqlite3
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from psycopg2 import errors

from schema import FilmWork, Genre, Person, GenreFilmWork, PersonFilmWork


def drop_all_tables():
    with psycopg2.connect(**dsl) as pg_conn, pg_conn.cursor() as cursor:
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
        cursor.execute(query)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres."""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    data = sqlite_extractor.extract_movies()
    postgres_saver.save_all_data(data)


class SQLiteExtractor:
    def __init__(self, connection):
        self.connection = connection

    def read_sqlite_tables_name(self) -> tuple:
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type = \"table\"")
        list_of_table_names = [data[0] for data in cursor.fetchall()]
        self.extract_tables(list_of_table_names, cursor)

    def extract_tables(
            self,
            list_of_table_names: list,
            cursor: sqlite3.Cursor
    ):
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
                self.write_to_csv(result, table_name, table_columns)

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
                self.write_to_csv(result, table_name, table_columns)

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
                self.write_to_csv(result, table_name, table_columns)

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
                self.write_to_csv(result, table_name, table_columns)

            elif table_name == 'genre':
                table_columns = ['id', 'name', 'description',
                                 'created', 'modified']

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
                self.write_to_csv(result, table_name, table_columns)

    def write_to_csv(self, result: list, table_name: str, table_columns: list):
        with open(f'{BASE_DIR/table_name}.csv', 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(table_columns)
            match table_name:
                case 'person_film_work':
                    writer.writerows([(i.id, i.film_work_id, i.person_id, i.role, i.created) for i in result])
    
                case 'person':
                    writer.writerows([(i.id, i.full_name, i.created, i.modified) for i in result])

                case 'film_work':
                    writer.writerows([(i.id, i.title, i.description, i.creation_date, i.rating, i.film_type, i.created, i.modified) for i in result])

                case 'genre_film_work':
                    writer.writerows([(i.id, i.film_work_id, i.genre_id, i.created) for i in result])

                case 'genre':
                    writer.writerows([(i.id, i.name, i.description, i.created, i.modified) for i in result])


    def extract_movies(self):
        self.read_sqlite_tables_name()
        path_all_csv_files = sorted(Path.cwd().rglob('*.csv'))
        return path_all_csv_files


class PostgresSaver:
    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn
        self.cursor = self.pg_conn.cursor()

    def create_tables(self):
        with open(ddl_file_path) as file:
            ddl_file = ' '.join(
                line.strip() for line in file.readlines()).split('  ')

        for command_sql in ddl_file:
            try:
                self.cursor.execute(command_sql)
            except (errors.DuplicateObject, errors.InFailedSqlTransaction):
                return

    def fill_the_table(self, csv_file_path: Path):
        table_name = Path(csv_file_path).parts[-1][:-4]  # Delete suffix '.csv'
        query = f"""COPY {table_name} FROM STDIN DELIMITER ',' CSV HEADER"""
        try:
            self.cursor.copy_expert(query, open(csv_file_path, 'r'))
            self.cursor.execute(f"""SELECT COUNT(id) FROM {table_name}""")
            result = self.cursor.fetchone()
            print(f'Результат выполнения команды COPY {table_name} ', result)
        except (errors.UniqueViolation, errors.InFailedSqlTransaction):
            return

    def save_all_data(self, data):
        drop_all_tables()
        self.create_tables()
        for csv_file_path in data:
            self.fill_the_table(csv_file_path)


if __name__ == '__main__':
    BASE_DIR = Path(__file__).resolve().parent
    db_sqlite_path = Path.joinpath(BASE_DIR, 'db.sqlite')
    ddl_file_path = Path(__file__).resolve().parent.parent.joinpath(
        'schema_design/movies_database.ddl'
    )

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
        load_from_sqlite(sqlite_conn, pg_conn)
