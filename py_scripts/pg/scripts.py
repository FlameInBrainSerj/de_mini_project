from py_scripts.pg.utils import _execute_sql_script
from py_scripts.env import REPO_ROOT_PATH, SQL_SCRIPTS_PATH, ANTIFRAUD_SQL_SCRIPTS_PATH


def create_tables() -> None:
    _execute_sql_script(REPO_ROOT_PATH / "main.ddl")


def delete_tables() -> None:
    _execute_sql_script(SQL_SCRIPTS_PATH / "delete_tables.sql")


def truncate_stg_tables() -> None:
    _execute_sql_script(SQL_SCRIPTS_PATH / "truncate_stg.sql")


def run_scd1_pipeline() -> None:
    _execute_sql_script(SQL_SCRIPTS_PATH / "scd1_transform.sql")


def run_antifraud_identification_script(script_name: str) -> None:
    _execute_sql_script(ANTIFRAUD_SQL_SCRIPTS_PATH / script_name)
