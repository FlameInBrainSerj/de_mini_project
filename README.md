# DE Mini Project

To build the Airflow image, run:
```bash
docker build ./dockerfiles/airflow/
```

To deploy the project, run and wait for 5-10 minutes:
```bash
docker compose up -d
```

To undeploy the project, run:
```bash
docker compose down 
```

To undeploy the project and delete volumes, run:
```bash
docker compose down -v
```

To run Pipeline follow these steps:
- Go to http://localhost:8084/home to access Airflow UI
- Enter the login `airflow` and password `airflow`
- Find the DAG `data_processing_pipeline`
- Press the button to run the DAG:
    - If you want to run the DAG for the specific date, enter the date in format `DDMMYYYY`
    - If you want to run the DAG for today, keep the field empty

To delete all tables, run:
```bash
python ./delete_tables.py
```
