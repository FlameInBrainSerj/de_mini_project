from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

from py_scripts.local.utils import archivise_file
from py_scripts.local.read import (
    read_transactions,
    read_terminals,
    read_passport_blacklist,
)
from py_scripts.pg.scripts import (
    create_tables,
    truncate_stg_tables,
    stg_dim_scd1_to_dwh,
    stg_fact_to_dwh,
    antifraud_identification_script,
)
from py_scripts.pg.read import (
    read_clients_table,
    read_cards_table,
    read_accounts_table,
)
from py_scripts.pg.write import write_to_db


def local_data_to_stg(**kwargs):
    date = kwargs["params"]["date"]
    if date is None:
        date = datetime.now().strftime("%d%m%Y")

    transactions_data = read_transactions(date)
    terminals_data = read_terminals(date)
    passport_blacklist_data = read_passport_blacklist(date)

    write_to_db(transactions_data, "sskr_stg_transactions")
    write_to_db(terminals_data, "sskr_stg_terminals")
    write_to_db(passport_blacklist_data, "sskr_stg_blacklist")


def pg_data_to_stg(**kwargs):
    clients_data = read_clients_table()
    cards_data = read_cards_table()
    accounts_data = read_accounts_table()

    write_to_db(clients_data, "sskr_stg_clients")
    write_to_db(cards_data, "sskr_stg_cards")
    write_to_db(accounts_data, "sskr_stg_accounts")


def archive_processed_files(**kwargs):
    date = kwargs["params"]["date"]
    if date is None:
        date = datetime.now().strftime("%d%m%Y")

    archivise_file("passport_blacklist", f"passport_blacklist_{date}.xlsx")
    archivise_file("terminals", f"terminals_{date}.xlsx")
    archivise_file("transactions", f"transactions_{date}.txt")


def antifraud_scripts(**kwargs):
    antifraud_identification_script("blocked_or_outdated_passport.sql")
    antifraud_identification_script("invalid_contract.sql")
    antifraud_identification_script("many_cities_in_one_hour.sql")
    antifraud_identification_script("sum_fit.sql")


dag = DAG(
    dag_id="data_processing_pipeline",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
    },
    description="Pipeline for processing and storing data",
    schedule_interval="0 3 * * *",
    start_date=datetime(2021, 3, 1),
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "date": Param(
            default=None,
            type=["null", "string"],
            title="Pass date",
            description_md="Pass the date in format DDMMYYYY",
        ),
    },
)

create_tables_task = PythonOperator(
    task_id="create_tables_if_not_exists",
    python_callable=create_tables,
    provide_context=True,
    dag=dag,
)

truncate_stg_tables_task = PythonOperator(
    task_id="truncate_stg_tables",
    python_callable=truncate_stg_tables,
    provide_context=True,
    dag=dag,
)

local_data_to_stg_task = PythonOperator(
    task_id="local_data_to_stg",
    python_callable=local_data_to_stg,
    provide_context=True,
    dag=dag,
)

pg_data_to_stg_task = PythonOperator(
    task_id="pg_data_to_stg",
    python_callable=pg_data_to_stg,
    provide_context=True,
    dag=dag,
)

stg_dim_scd1_to_dwh_task = PythonOperator(
    task_id="stg_dim_scd1_to_dwh",
    python_callable=stg_dim_scd1_to_dwh,
    provide_context=True,
    dag=dag,
)

stg_fact_to_dwh_task = PythonOperator(
    task_id="stg_fact_to_dwh",
    python_callable=stg_fact_to_dwh,
    provide_context=True,
    dag=dag,
)

antifraud_scripts_task = PythonOperator(
    task_id="antifraud_scripts",
    python_callable=antifraud_scripts,
    provide_context=True,
    dag=dag,
)


archive_processed_files_task = PythonOperator(
    task_id="archive_processed_files",
    python_callable=archive_processed_files,
    provide_context=True,
    dag=dag,
)

(
    create_tables_task
    >> truncate_stg_tables_task
    >> [local_data_to_stg_task, pg_data_to_stg_task]
    >> archive_processed_files_task
    >> stg_dim_scd1_to_dwh_task
    >> stg_fact_to_dwh_task
    >> antifraud_scripts_task
)
