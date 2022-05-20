# 주식 데이터 파이프라인과 대시보드 구현

## 개요
Airflow와 AWS를 활용하여 주식 일일 배치 데이터 파이프라인 및 대시보드 구현


## 사용 기술
Python  
Airflow  
AWS S3  
AWS Glue  
AWS Athena  
Tableau  

## 아키텍쳐
![image](https://user-images.githubusercontent.com/64902669/166087604-f49e652f-f756-4e4e-a043-5b9099d314a6.png)


## DAG 구조
  ### 1. roks_stock_DAG
  ![image](https://user-images.githubusercontent.com/64902669/166092172-18b403ac-a32d-418a-afd7-41ffe49fe5dd.png)

  ### 2. krx_holiday_DAG
  - 단일 태스크

## 대시보드 예시
![image](https://user-images.githubusercontent.com/64902669/166093967-9227fd3f-99c0-4ec3-a0c5-7a4944e050b4.png)

## 기능
- 일일 KOSPI 주식 주가 및 거래량 확인 가능
- 전일 대비 가장 많이 상승된 종목 및 업종 확인 가능
- 전일 전체, 기관, 외국인별 매수량이 가장 높은 종목 확인 가능
- 종목별 주가 차트와 거래량 차트 확인 가능 


## 각 DAG 리뷰
### roks_stock_DAG
- start_date : 2022년 4월 1일
- schedule_interval : UTC 기준 매일 13시(한국 시간: 22시), cron expression : 0 13 * * *
- catchup : True, 과거 데이터 얻기 위해 backfill True

  ### 1. check_holiday_task
  목적 : KOSPI 주식 시장 휴장일 확인
  - S3에 저장된 KOSPI 휴장일 데이터를 가져온 후, 해당 날짜가 휴장일인지 확인
  - ShortCircuitOperator를 활용하여 휴장일 일 경우 Skip , 휴장일이 아닐 경우 Pass

  ### 2. get_TARGET_task (stock, price, trade)
  목적 : KRX 정보데이터 시스템을 크롤링하여 각 데이터를 얻고, S3에 저장
  - KRX 정보데이터 시스템에서 해당 날짜를 기준으로 원하는 데이터를 크롤링
  - 얻은 데이터를 parquet형으로 변환 후 s3에 저장

  ### 3. glue_TARGET_task (stock, price, trade)
  목적 : s3에 저장된 데이터를 AWS Glue Crawler를 이용하여 크롤링
  - s3에 신규로 저장된 데이터를 glue를 통해 크롤링
  - 데이터 카탈로그 생성 및 아테나 테이블 생성


### krx_holiday_DAG
- start_date : 2018년 1월 1일
- schedule_interval : UTC 기준 매년 1월 1일 10시 (한국 시간: 19시), cron expression : 0 10 1 1 *

  #### 1. get_hoilday_task
  목적 : KRX 정보데이터 시스템을 크롤링하여 각 년도 KOSPI 휴장일 데이터 얻고, S3에 저장
  - KRX 정보데이터 시스템에서 년도를 기준으로 휴장일 데이터 얻기
  - 얻은 데이터(excel)를 csv로 변환 후 s3에 저장


## 태블로 시각화
목적 : Tableau를 활용하여 주식 대시보드 구현
- AWS Athena를 연결
- stock_info 테이블 기준으로 price와 trade 테이블을 각각 left join
- 차트 구현 : 주가(시가,종가,고가,저가) 차트와 거래량 차트 구현
- TOP 종목 : 각 테마에 맞는 TOP 5 종목 표 구현

## 아쉬운 점
- 가장 데이터가 잘 구축되어 있는 네이버, 다음 금융 등은 robots.txt에 따라 크롤링이 불가
- 크롤링의 한계로 인해, 실시간 데이터 파이프라인 및 대시보드 구현은 하지 못함
- 태블로 차트 특성 상 주식 차트에 5, 20, 60, 120일선 구현하지 못함
- 액면 분할, 증자, 거래 정지 등 상황에 대해 대처하지 못함

## 이후 발전 사항
- 주식 데이터 뿐만 아니라 검색량, 뉴스 개수 등 다른 지표도 함꼐 활용해보면 재밌을 듯
- (가격, 거래량) 급락주, 급등주 또는 보유 종목에 대해 매일 아침에 알려주는 기능 구현하면 재밌을 듯  
- 주식은 보안(?), 특성(?) 상 실시간 파이프라인 구현이 까다롭기 때문에 가상화폐(비트코인) 데이터로 실시간 파이프라인 구현해보기 
