import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from io import BytesIO

# Get OTP
def get_hoilday_otp(headers, year):
    otp_url = "https://open.krx.co.kr/contents/COM/GenerateOTP.jspx?"

    params = {
        'name' : "fileDown",
        'filetype' : 'xls',
        'url' : 'MKD/01/0110/01100305/mkd01100305_01',
        'search_bas_yy' : year,
        'gridTp' : "KRX",
        'pagePath' : '/contents/MKD/01/0110/01100305/MKD01100305.jsp'
    }
    otp = requests.post(otp_url, params=params, headers=headers).text

    return otp

# Get Holiday
def get_holiday(**context):
    date = context["tomorrow_ds"]
    year = date.split("-")[0]

    headers = {
    'User-Agent':
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    'Referer' :
        "https://open.krx.co.kr/contents/MKD/01/0110/01100305/MKD01100305.jsp"}

    otp = get_hoilday_otp(headers, year)

    get_url = "https://file.krx.co.kr/download.jspx"
    result = requests.get(get_url, params={'code': otp}, headers=headers)

    krx_holiday_xls = pd.read_excel(BytesIO(result.content))
    krx_holiday_xls.to_csv(f"/tmp/{year}_krx_holiday.csv", index=False, encoding='utf-8')

    # load to s3
    s3_hook = S3Hook("s3_conn")
    s3_hook.load_file(f"/tmp/{year}_krx_holiday.csv", 
        key=f'krx-holiday/dt={year}/krx_holiday.csv',
        bucket_name="roks-stock")

default_args = {
    'owner' : "Airflow",
    'start_date' : datetime(2018,1,1)
}

with DAG('get_krx_hoilday',
    default_args=default_args,
    # Jan 1st 10:00 yearly
    schedule_interval='0 10 1 1 *',
    catchup=True) as dag:

    holiday_task = PythonOperator(
        task_id='get_hoilday_task',
        python_callable=get_holiday,
        provide_context=True,
        dag=dag
    )

holiday_task


