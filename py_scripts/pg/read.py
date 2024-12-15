import pandas as pd

from py_scripts.env import PG_ENGINE


def read_clients_table() -> pd.DataFrame:
    query = """
        SELECT
            TRIM(client_id)         AS client_id
            , TRIM(last_name)       AS last_name
            , TRIM(first_name)      AS first_name
            , TRIM(patronymic)      AS patronymic
            , date_of_birth
            , TRIM(passport_num)    AS passport_num
            , passport_valid_to
            , TRIM(phone)           AS phone
            , create_dt
            , update_dt
        FROM
            info.clients
        ;
    """

    return pd.read_sql(query, PG_ENGINE)


def read_cards_table() -> pd.DataFrame:
    query = """
        SELECT
            TRIM(card_num)          AS card_num
            , TRIM(account)         AS account
            , create_dt
            , update_dt
        FROM
            info.cards
        ;
    """

    return pd.read_sql(query, PG_ENGINE).rename(columns={"account": "account_num"})


def read_accounts_table() -> pd.DataFrame:
    query = """
        SELECT
            TRIM(account)           AS account
            , valid_to
            , TRIM(client)          AS client
            , create_dt
            , update_dt
        FROM
            info.accounts
        ;
    """

    return pd.read_sql(query, PG_ENGINE).rename(columns={"account": "account_num"})
