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
STG_TO_DWH_SCRIPTS_PATH = SQL_SCRIPTS_PATH / "stg_to_dwh"
STG_TO_DWH_DIM_SCRIPTS_PATH = STG_TO_DWH_SCRIPTS_PATH / "dim"
STG_TO_DWH_FACT_SCRIPTS_PATH = STG_TO_DWH_SCRIPTS_PATH / "fact"
ANTIFRAUD_SQL_SCRIPTS_PATH = SQL_SCRIPTS_PATH / "antifraud"
