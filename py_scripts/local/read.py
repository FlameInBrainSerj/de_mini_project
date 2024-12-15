from datetime import datetime

import pandas as pd

from py_scripts.env import DATA_SOURCE_PATH


def read_transactions(date: str) -> None:
    transactions_data = pd.read_csv(
        DATA_SOURCE_PATH / "transactions" / f"transactions_{date}.txt",
        delimiter=";",
    ).rename(
        columns={
            "transaction_id": "trans_id",
            "transaction_date": "trans_date",
            "amount": "amt",
        }
    )

    transactions_data["amt"] = transactions_data["amt"].map(
        lambda x: x.replace(",", ".")
    )

    return transactions_data


def read_terminals(date: str) -> None:
    terminals_data = pd.read_excel(
        DATA_SOURCE_PATH / "terminals" / f"terminals_{date}.xlsx",
        sheet_name="terminals",
    )
    terminals_data["file_date"] = f"{date[4:]}-{date[2:4]}-{date[:2]}"

    return terminals_data


def read_passport_blacklist(date: str) -> None:
    passport_blacklist_data = pd.read_excel(
        DATA_SOURCE_PATH / "passport_blacklist" / f"passport_blacklist_{date}.xlsx",
        sheet_name="blacklist",
    ).rename(columns={"passport": "passport_num", "date": "entry_dt"})

    return passport_blacklist_data
