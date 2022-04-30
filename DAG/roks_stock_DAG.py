import pendulum

from airflow import DAG
from airflow.utils.helpers import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from datetime import datetime
from operators.get_stock_operator import GetStockOperator
from operators.get_price_operator import GetPriceOperator
from operators.get_trade_operator import GetTradeOperator

# ShortCircuitOperator execution
## 휴장일 체크
def execute(**context):
    # DAG의 excution_time 내일 기준 -> 당일 기준 실행 위해
    date = context["tomorrow_ds"]
    korea_date = date.replace("-", "")
    korea_year = date.split("-")[0]
    
    # Connect to S3 
    s3_hook = S3Hook('s3_conn')
    # Get Holiday Info
    holiday_info = s3_hook.read_key(
        key=f'krx-holiday/dt={korea_year}/krx_holiday.csv',
        bucket_name="roks-stock")

    # Exclude frist row(Column name)
    holiday_info = holiday_info.split('\n')[1:]
    # get only holiday date
    holidays = [info.split(',')[0] for info in holiday_info]
    # refine holidays 
    holidays = [day.replace("-", "") for day in holidays]

    if korea_date in holidays or datetime.strptime(korea_date, "%Y%m%d").weekday() >= 5:
        return False
    else:
        return True


# DAG default args
default_args = {
    'owner' : "Airflow",
    # UTC 기준이기 때문에 한국 시간 기준보다 -9시간
    # 한국 시간으로 당일 22시에 시작할 것 ->  UTC 기준 13시
    # 4월 1일 기준 시작
    'start_date' : datetime(2022, 3, 31, 13, 0, 0),
    'wait_for_downstream' : True,
    'max_active_runs' : 1
}

# Define DAG
with DAG("stock_dag",
    default_args=default_args,
    # 13:00 daily
    schedule_interval='0 13 * * *',
    # Backfill
    catchup=True) as dag:
    
    # start
    start = DummyOperator(task_id="start")
    # check holiday
    check_holiday = ShortCircuitOperator(
        task_id='check_holiday_task', 
        provide_context=True,
        python_callable=execute, 
        dag=dag
    )
    holiday = DummyOperator(task_id="is_holiday")

    # Get Stock Data
    ## Get Stock Info & Sector to S3
    get_info_task = GetStockOperator(
        task_id="get_info_task",
        dag=dag
    )
    ## Get Stock Price to S3
    get_price_task = GetPriceOperator(
        task_id="get_price_task",
        dag=dag
    )
    ## Get Stock Trade to S3
    get_trade_task = GetTradeOperator(
        task_id="get_trade_task",
        dag=dag
    )

    get_data_check = DummyOperator(task_id="get_data_check")
    
    # Exectue Glue Crawler
    glue_info = AwsGlueCrawlerOperator(
        task_id="glue_info_task",
        config={"Name" : "glue-stock-info"},
        aws_conn_id="glue_conn"
    )
    glue_price = AwsGlueCrawlerOperator(
        task_id="glue_price_task",
        config={"Name" : "glue-stock-price"},
        aws_conn_id="glue_conn"
    )
    glue_trade = AwsGlueCrawlerOperator(
        task_id="glue_trade_task",
        config={"Name" : "glue-stock-trade"},
        aws_conn_id="glue_conn"
    )
    glue_check = DummyOperator(task_id="glue_check")

    # END
    end = DummyOperator(task_id="end")

chain(
    start,
    check_holiday,
    (get_info_task, get_price_task, get_trade_task),
    get_data_check,
    (glue_info, glue_price, glue_trade),
    glue_check,
    end
)
# Is holiday
start >> holiday >> end