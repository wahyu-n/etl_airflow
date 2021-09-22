from datetime import datetime
import pandas as panda
import datetime
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

args = {
    'owner': 'Airflow',
    'start_date': datetime.datetime(2021, 9, 20)
}

dag = DAG(
    dag_id='etl_airflow',
    default_args=args,
    schedule_interval="@daily",
)

def web_scraping(**context):
    data = panda.read_html("https://id.wikipedia.org/wiki/Daftar_orang_terkaya_di_Indonesia")
    data2020 = data[7]
    data2020.to_csv("list_orang_terkaya_di_indonesia.csv")

scraping_web = PythonOperator(
    task_id='website_scraping',
    provide_context=True,
    python_callable=web_scraping,
    dag=dag,
)

send_email = EmailOperator(
        task_id='send_email',
        to='fia.digitalskola@gmail.com',
        subject=' Wahyu_DigitalSkola_Airflow',
        html_content=""" <h3>Wahyu Nugraha</h3> """,
        files=['list_orang_terkaya_di_indonesia.csv'],
        dag=dag
)

scraping_web >> send_email
