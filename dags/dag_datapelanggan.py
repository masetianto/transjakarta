
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'transjakarta',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres:5432/transjakarta')

def extract_data(**context):
    bus = pd.read_csv('/opt/airflow/data/input/dummy_transaksi_bus.csv')
    halte = pd.read_csv('/opt/airflow/data/input/dummy_transaksi_halte.csv')
    bus.to_csv('/opt/airflow/data/intermediate/bus.csv', index=False)
    halte.to_csv('/opt/airflow/data/intermediate/halte.csv', index=False)
    print('Extract done')

def transform_data(**context):
    bus = pd.read_csv('/opt/airflow/data/intermediate/bus.csv')
    halte = pd.read_csv('/opt/airflow/data/intermediate/halte.csv')
    df = pd.concat([bus, halte], ignore_index=True)
    df.drop_duplicates(inplace=True)

    # Standarisasi no_body_var: e.g. 'BRT 15' -> 'BRT-015', 'LGS_251A' -> 'LGS-251'
    df['no_body_var'] = df['no_body_var'].astype(str).str.upper().str.replace('_', '-').str.replace(' ', '-')
    def fmt_nb(x):
        parts = x.split('-')
        prefix = parts[0]
        suffix = ''.join(filter(str.isdigit, parts[-1]))
        if suffix == '':
            suffix = '0'
        return f"{prefix}-{int(suffix):03d}"
    df['no_body_var'] = df['no_body_var'].apply(fmt_nb)

    df_pelanggan = df[df['status_var'] == 'S']
    df_pelanggan.to_csv('/opt/airflow/data/intermediate/pelanggan_clean.csv', index=False)
    print('Transform done')

def load_data(**context):
    df = pd.read_csv('/opt/airflow/data/intermediate/pelanggan_clean.csv')

    by_card = df.groupby(['tanggal', 'card_type', 'gate_in_boo']).agg({'amount':'sum', 'pelanggan_id':'count'}).reset_index()
    by_card.rename(columns={'pelanggan_id':'jumlah_pelanggan', 'amount':'total_amount'}, inplace=True)
    by_card.to_csv('/opt/airflow/data/output/pelanggan_by_cardtype.csv', index=False)
    by_card.to_sql('pelanggan_by_cardtype', con=engine, if_exists='replace', index=False)

    by_route = df.groupby(['tanggal', 'route_code', 'route_name', 'gate_in_boo']).agg({'amount':'sum', 'pelanggan_id':'count'}).reset_index()
    by_route.rename(columns={'pelanggan_id':'jumlah_pelanggan', 'amount':'total_amount'}, inplace=True)
    by_route.to_csv('/opt/airflow/data/output/pelanggan_by_route.csv', index=False)
    by_route.to_sql('pelanggan_by_route', con=engine, if_exists='replace', index=False)

    by_tarif = df.groupby(['tanggal', 'tarif', 'gate_in_boo']).agg({'amount':'sum', 'pelanggan_id':'count'}).reset_index()
    by_tarif.rename(columns={'pelanggan_id':'jumlah_pelanggan', 'amount':'total_amount'}, inplace=True)
    by_tarif.to_csv('/opt/airflow/data/output/pelanggan_by_tarif.csv', index=False)
    by_tarif.to_sql('pelanggan_by_tarif', con=engine, if_exists='replace', index=False)

    print('Load done')

with DAG(
    'dag_datapelanggan',
    default_args=default_args,
    description='ETL Transjakarta Pelanggan Harian',
    schedule_interval='0 7 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id='load_task',
        python_callable=load_data
    )

    extract >> transform >> load
