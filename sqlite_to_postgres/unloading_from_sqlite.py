import sqlite3
import csv

# # Задаём путь к файлу с базой данных
# db_path = 'db.sqlite'
# # Устанавливаем соединение с БД
# conn = sqlite3.connect(db_path)
# # По-умолчанию SQLite возвращает строки в виде кортежа значений. Эта строка указывает, что данные должны быть в формате «ключ-значение»
# conn.row_factory = sqlite3.Row  
# # Получаем курсор
# curs = conn.cursor()
# # Формируем запрос. Внутри execute находится обычный SQL-запрос
# curs.execute("SELECT * FROM film_work;")
# # Получаем данные
# data = curs.fetchall()
# # Рассматриваем первую запись
# print(dict(data[0]))
# # Разрываем соединение с БД
# conn.close()
from pprint import pprint
from models import FilmWork

db_path = 'db.sqlite'
with sqlite3.connect(db_path) as conn:
    conn.row_factory = sqlite3.Row
    curs = conn.cursor()
    curs.execute("SELECT * FROM film_work;")
    # curs.execute("SELECT name FROM sqlite_master WHERE type = \"table\"")
    data = curs.fetchall()
    # pprint(dict(data[0]).keys())
    # pprint(data)
    ku = dict(data[0])#.items()
    kl = []
    # for k, v in ku:
    #     if k != 'file_path':
    #         kl.append(v)
    fw = FilmWork(
        id=ku['id'],
        title=ku['title'],
        description=ku['description'],
        creation_date=ku['creation_date'],
        rating=ku['rating'],
        type=ku['type'],
        created=ku['created_at'],
        modified=ku['updated_at']
    )
    print(fw)

import sqlite3
import csv
import os.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "db.sqlite")
# conn = sqlite3.connect(db_path)
# c = conn.cursor()
# c.execute("SELECT rowid, * FROM inventory")    
# columns = [column[0] for column in c.description]
# results = []
# for row in c.fetchall():
#     results.append(dict(zip(columns, row)))
# with open("output.csv", "w", newline='') as new_file:
#     fieldnames = columns
#     writer = csv.DictWriter(new_file,fieldnames=fieldnames)
#     writer.writeheader()
#     for line in results:
#         writer.writerow(line)
# conn.close()
# print(db_path)