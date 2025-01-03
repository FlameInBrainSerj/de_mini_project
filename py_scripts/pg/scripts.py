from py_scripts.pg.utils import _execute_sql_script
from py_scripts.env import (
    REPO_ROOT_PATH,
    SQL_SCRIPTS_PATH,
    ANTIFRAUD_SQL_SCRIPTS_PATH,
    STG_TO_DWH_DIM_SCD1_SCRIPTS_PATH,
    STG_TO_DWH_DIM_SCD2_SCRIPTS_PATH,
    STG_TO_DWH_FACT_SCRIPTS_PATH,
)


def create_tables() -> None:
    _execute_sql_script(REPO_ROOT_PATH / "main.ddl")


def delete_tables() -> None:
    _execute_sql_script(SQL_SCRIPTS_PATH / "delete_tables.sql")


def truncate_stg_tables() -> None:
    _execute_sql_script(SQL_SCRIPTS_PATH / "truncate_stg.sql")


def stg_dim_scd1_to_dwh() -> None:
    _execute_sql_script(STG_TO_DWH_DIM_SCD1_SCRIPTS_PATH / "cards.sql")
    _execute_sql_script(STG_TO_DWH_DIM_SCD1_SCRIPTS_PATH / "accounts.sql")
    _execute_sql_script(STG_TO_DWH_DIM_SCD1_SCRIPTS_PATH / "clients.sql")
    _execute_sql_script(STG_TO_DWH_DIM_SCD1_SCRIPTS_PATH / "terminals.sql")


def stg_fact_to_dwh() -> None:
    _execute_sql_script(STG_TO_DWH_FACT_SCRIPTS_PATH / "transactions.sql")
    _execute_sql_script(STG_TO_DWH_FACT_SCRIPTS_PATH / "passport_blacklist.sql")


def antifraud_identification_script(script_name: str) -> None:
    _execute_sql_script(ANTIFRAUD_SQL_SCRIPTS_PATH / script_name)
