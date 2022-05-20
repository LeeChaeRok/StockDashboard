from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from operators.krx_api import GetDataFromKrx 
from io import BytesIO

class GetPriceOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
        data=None, 
        *args,
        **kwargs) -> None:

        super().__init__(*args, **kwargs)

        self.data = data


    def get_stock_price(self, date):
        price_params = {
            'locale': 'ko_KR',
            'mktId': 'STK',
            'trdDd': date,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01501'
        }

        stock_price_df = GetDataFromKrx(price_params)

        # get only need columns & rename columns
        stock_price_df = stock_price_df[['종목코드', '시가', '종가', '고가', '저가', '상장주식수']]
        stock_price_df.columns = ["stock_code", 'opening_price', 'closing_price', 'high_price', 'low_price', 'total_shares']
        print("Total Stock Infos : ", len(stock_price_df))

        return stock_price_df

    # 가격 정보 가져오기
    def execute(self, context):
        # DAG의 excution_time 내일 기준 
        date = context["tomorrow_ds_nodash"]

        # Get Data
        price_df = self.get_stock_price(date)

        # DataFrame to parquet
        info_buffer = BytesIO()
        price_df.to_parquet(info_buffer, engine='pyarrow', index=False)
        
        # connect to s3
        s3_hook = S3Hook("s3_conn")

        # load to s3
        s3_hook.load_bytes(info_buffer.getvalue(), 
            key=f'stock-price/dt={date}/stock_price.parquet',
            bucket_name="roks-stock")

    
