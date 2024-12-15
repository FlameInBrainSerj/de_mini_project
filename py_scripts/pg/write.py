import pandas as pd
from py_scripts.env import PG_ENGINE


def write_to_db(res: pd.DataFrame, table_name: str) -> None:
    res.to_sql(
        table_name,
        PG_ENGINE,
        schema="public",
        if_exists="append",
        index=False,
    )
