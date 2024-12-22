import argparse

import pandas as pd

from py_scripts.local.utils import archivise_file
from py_scripts.local.read import (
    read_transactions,
    read_terminals,
    read_passport_blacklist,
)
from py_scripts.pg.scripts import (
    create_tables,
    delete_tables,
    truncate_stg_tables,
    run_stg_to_dwh_pipeline,
    run_antifraud_identification_script,
)
from py_scripts.pg.read import (
    read_clients_table,
    read_cards_table,
    read_accounts_table,
)
from py_scripts.pg.write import write_to_db

parser = argparse.ArgumentParser()
parser.add_argument(
    "--date",
    type=str,
    help="Pass date in form DDMMYYYY",
)

DATE = parser.parse_args().date


def read_data_local() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    transactions_data = read_transactions(DATE)
    terminals_data = read_terminals(DATE)
    passport_blacklist_data = read_passport_blacklist(DATE)

    return (transactions_data, terminals_data, passport_blacklist_data)


def read_data_pg() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    clients_data = read_clients_table()
    cards_data = read_cards_table()
    accounts_data = read_accounts_table()

    return (clients_data, cards_data, accounts_data)


def write_data_to_pg(
    transactions_data: pd.DataFrame,
    terminals_data: pd.DataFrame,
    passport_blacklist_data: pd.DataFrame,
    clients_data: pd.DataFrame,
    cards_data: pd.DataFrame,
    accounts_data: pd.DataFrame,
) -> None:
    write_to_db(transactions_data, "sskr_stg_transactions")
    write_to_db(terminals_data, "sskr_stg_terminals")
    write_to_db(passport_blacklist_data, "sskr_stg_blacklist")

    write_to_db(clients_data, "sskr_stg_clients")
    write_to_db(cards_data, "sskr_stg_cards")
    write_to_db(accounts_data, "sskr_stg_accounts")


def archive_processed_files() -> None:
    archivise_file("passport_blacklist", f"passport_blacklist_{DATE}.xlsx")
    archivise_file("terminals", f"terminals_{DATE}.xlsx")
    archivise_file("transactions", f"transactions_{DATE}.txt")


def run_antifraud_scripts() -> None:
    pass
    run_antifraud_identification_script("blocked_or_outdated_passport.sql")
    run_antifraud_identification_script("invalid_contract.sql")
    run_antifraud_identification_script("many_cities_in_one_hour.sql")
    run_antifraud_identification_script("sum_fit.sql")


# TODO:
# - logical recheck
# - check: 3 consecutive executions are correct (no duplication, etc.)
# - last antifraud
# - scd2


if __name__ == "__main__":
    delete_tables()
    # create_tables()

    # truncate_stg_tables()

    # (transactions_data, terminals_data, passport_blacklist_data) = read_data_local()
    # (clients_data, cards_data, accounts_data) = read_data_pg()

    # write_data_to_pg(
    #     transactions_data,
    #     terminals_data,
    #     passport_blacklist_data,
    #     clients_data,
    #     cards_data,
    #     accounts_data,
    # )

    # run_stg_to_dwh_pipeline()

    # run_antifraud_scripts()

    # archive_processed_files()
