import pandas as pd

from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from operators.krx_api import GetDataFromKrx 
from io import BytesIO

# Define Operator
class GetTradeOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
        data=None, 
        *args,
        **kwargs) -> None:

        super().__init__(*args, **kwargs)

        self.data = data


    def get_stock_trade(self, date, target):
        # select code by target 
        invest_code = ""
        if target == "total":
            invest_code = "9999"
        elif target == "inst":
            invest_code = "7050"
        elif target == "foreign":
            invest_code = "9000"
        else:
            raise

        trade_params = {
            'locale': 'ko_KR',
            'mktId': 'STK',
            'invstTpCd' : invest_code,
            'strtDd' : date,
            'endDd' : date,
            'share' : '1',
            'money' : '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT02401'
        }

        trade_df = GetDataFromKrx(trade_params)

        return trade_df

    def convert_to_parquet(self, df):
        # DataFrame to parquet
        buffer = BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)

        return buffer

    def execute(self, context):
        # DAG의 excution_time 내일 기준 
        date = context["tomorrow_ds_nodash"]
        
        # Get Data
        total_trade_df = self.get_stock_trade(date, "total")
        inst_trade_df = self.get_stock_trade(date, "inst")
        foreign_trade_df = self.get_stock_trade(date, "foreign")

        # get only need columns & rename columns
        total_trade_df = total_trade_df[['종목코드', '거래량_매수', '거래대금_매수']]
        total_trade_df.columns = ['stock_code', 'total_trade_volume', 'total_trade_amount']

        inst_trade_df = inst_trade_df[['종목코드', '거래량_매수', '거래량_매도', '거래대금_매수', '거래대금_매도']]
        inst_trade_df.columns = ['stock_code', 'buy_volume_inst', 'sell_volume_inst', 'buy_amount_inst', 'sell_amount_inst']

        foreign_trade_df = foreign_trade_df[['종목코드', '거래량_매수', '거래량_매도', '거래대금_매수', '거래대금_매도']]
        foreign_trade_df.columns = ['stock_code', 'buy_volume_frgn', 'sell_volume_frgn', 'buy_amount_frgn', 'sell_amount_frgn']

        # make all_trade_df by join
        all_trade_df = pd.merge(total_trade_df, inst_trade_df, how='left', on='stock_code')
        all_trade_df = pd.merge(all_trade_df, foreign_trade_df, how='left', on='stock_code')

        # DataFrame to parquet
        total_buffer = self.convert_to_parquet(total_trade_df)
        inst_buffer = self.convert_to_parquet(inst_trade_df)
        foreign_buffer = self.convert_to_parquet(foreign_trade_df)
        all_buffer = self.convert_to_parquet(all_trade_df)
        
        # connect to s3
        s3_hook = S3Hook("s3_conn")

        # load to s3
        s3_hook.load_bytes(total_buffer.getvalue(), 
            key=f'stock-trade/total-trade/dt={date}/total_trade.parquet',
            bucket_name="roks-stock")

        s3_hook.load_bytes(inst_buffer.getvalue(), 
            key=f'stock-trade/institute-trade/dt={date}/institute_trade.parquet',
            bucket_name="roks-stock")

        s3_hook.load_bytes(foreign_buffer.getvalue(), 
            key=f'stock-trade/foreigner-trade/dt={date}/foreigner_trade.parquet',
            bucket_name="roks-stock")

        s3_hook.load_bytes(all_buffer.getvalue(), 
            key=f'stock-trade/stock-trade/dt={date}/stock_trade.parquet',
            bucket_name="roks-stock")

    
