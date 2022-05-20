import pandas as pd

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from operators.krx_api import GetDataFromKrx 
from io import BytesIO

# Define Operator
class GetStockOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
        data=None, 
        *args,
        **kwargs) -> None:

        super().__init__(*args, **kwargs)

        self.data = data

    def get_stock_info(self):
        info_params = {
            'locale': 'ko_KR',
            'mktId': 'STK',
            'share': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'
        }

        stock_info_df = GetDataFromKrx(info_params)

        # get only need columns & rename columns
        stock_info_df = stock_info_df[['단축코드', '표준코드', '한글 종목명', '시장구분']]
        stock_info_df.columns = ["stock_code", "isin", 'stock_name', 'market']

        # refine date and name
        stock_info_df['stock_name'] = stock_info_df['stock_name'].str.replace("우선주", "우").str.replace("보통주", "")

        return stock_info_df

    def get_stock_sector(self, date):
        sector_params = {
            'locale': 'ko_KR',
            'mktId': 'STK',
            # 기준 날짜
            'trdDd': date,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03901'
        }

        stock_sector_df = GetDataFromKrx(sector_params)

        # get only need columns & rename columns
        stock_sector_df = stock_sector_df[['종목코드', '업종명']]
        stock_sector_df.columns = ['stock_code', 'sector']

        return stock_sector_df

    def execute(self, context):
        # DAG의 excution_time 내일 기준 
        date = context["tomorrow_ds_nodash"]

        # Get Data
        info_df = self.get_stock_info()
        sector_df = self.get_stock_sector(date)

        # merge
        stock_df = pd.merge(info_df, sector_df, how='left', on='stock_code')

        # DataFrame to parquet
        info_buffer = BytesIO()
        stock_df.to_parquet(info_buffer, engine='pyarrow', index=False)
        
        # connect to s3
        s3_hook = S3Hook("s3_conn")

        # load to s3
        s3_hook.load_bytes(info_buffer.getvalue(), 
            key=f'stock-info/dt={date}/stock_info.parquet',
            bucket_name="roks-stock")

    
