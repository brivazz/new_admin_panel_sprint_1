import os
import sys
import logging
from logging import StreamHandler, Formatter
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = StreamHandler(stream=sys.stdout)
handler.setFormatter(Formatter(fmt='[%(asctime)s: %(levelname)s] %(message)s'))
logger.addHandler(handler)

logger.debug('debug information')


dsn = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432),
    'options': '-c search_path=content',
}

BASE_DIR = Path(__file__).resolve().parent
db_sqlite_path = Path.joinpath(BASE_DIR, 'db.sqlite')
ddl_file_path = Path(__file__).resolve().parent.parent.joinpath(
    'schema_design/movies_database.ddl'
)
