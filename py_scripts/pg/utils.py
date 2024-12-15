from sqlalchemy import text

from py_scripts.env import PG_ENGINE


def _read_sql_script(sql_file_path: str) -> str:
    with open(sql_file_path, "r") as file:
        sql_script = file.read()

    return sql_script


def _execute_sql_script(sql_file_path: str) -> None:
    with PG_ENGINE.connect() as connection:
        sql_script = _read_sql_script(sql_file_path)
        connection.execute(text(sql_script))
