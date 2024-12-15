import pandas as pd

from py_scripts.env import PG_ENGINE


def read_clients_table() -> pd.DataFrame:
    query = """
        SELECT
            client_id
            , last_name
            , first_name
            , patronymic
            , date_of_birth
            , passport_num
            , passport_valid_to
            , phone
            , create_dt
            , update_dt
        FROM
            info.clients
        """

    return pd.read_sql(query, PG_ENGINE)


def read_cards_table() -> pd.DataFrame:
    query = """
        SELECT
            card_num
            , account
            , create_dt
            , update_dt
        FROM
            info.cards
        """

    return pd.read_sql(query, PG_ENGINE).rename(columns={"account": "account_num"})


def read_accounts_table() -> pd.DataFrame:
    query = """
        SELECT
            account
            , valid_to
            , client
            , create_dt
            , update_dt
        FROM
            info.accounts
        """

    return pd.read_sql(query, PG_ENGINE).rename(columns={"account": "account_num"})
