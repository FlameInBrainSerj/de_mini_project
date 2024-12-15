from pathlib import Path

from sqlalchemy import create_engine

HOST = "rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net"
PORT = 6432
DBNAME = "db"
USER = "hseguest"
PASSWORD = "hsepassword"
PG_ENGINE = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}")

REPO_ROOT_PATH = Path(__file__).parent.parent
DATA_SOURCE_PATH = REPO_ROOT_PATH / "source"
DATA_ARCHIVE_PATH = REPO_ROOT_PATH / "archive"

SQL_SCRIPTS_PATH = REPO_ROOT_PATH / "sql_scripts"
ANTIFRAUD_SQL_SCRIPTS_PATH = SQL_SCRIPTS_PATH / "antifraud"
