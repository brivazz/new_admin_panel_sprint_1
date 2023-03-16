import sqlite3
from pathlib import Path
from typing import List, Any
from contextlib import contextmanager


from settings import logger


@contextmanager
def open_sqlite_db(db_sqlite_path: Path):
    connection = sqlite3.connect(db_sqlite_path)
    try:
        yield connection.cursor()
    finally:
        connection.commit()
        connection.close()


def reformat_sqlite_fields(elem: list) -> dict:
    """
        Создаем функцию осуществления замены по различающимся полям данным.
        Список необходим для расширения, в случае добавления новых данных.
    """
    for item in elem:
        if 'created_at' in item.keys():
            item['created'] = item['created_at']
            del (item['created_at'])

        if 'updated_at' in item.keys():
            item['modified'] = item['updated_at']
            del (item['updated_at'])

        if 'type' in item.keys():
            item['film_type'] = item['type']
            del (item['type'])

        if 'file_path' in item.keys():
            del (item['file_path'])

    return item


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

                yield rows
        except Exception as exception:
            logger.error(exception)

    def format_dataclass_data(self, table_name: str, dataclass) -> List[Any]:
        data = self.load_sqlite(table_name)
        return [dataclass(**reformat_sqlite_fields(elem)) for elem in data]
