import requests
import pandas as pd

from io import BytesIO

def GetOtp(params):
    session = requests.Session()
    otp_response = session.post("http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd",
                                params=params,
                                headers={"Referer" : 
                                        "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201"})
    
    otp_response.raise_for_status()
    otp = otp_response.text
    
    return otp

def GetDataFromKrx(params):
    otp = GetOtp(params)
    
    session = requests.Session()
    response = session.post(url="http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd",
                            params = {'code': otp},
                            headers = {"Referer" : f"http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201"})

    response.raise_for_status()
    result_df = pd.read_csv(BytesIO(response.content), encoding='cp949')
    
    return result_df