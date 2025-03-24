import os
import sys
sys.path.append("D:/PythonProjects/airstock")

import subprocess
import time
from datetime import datetime as DateTime, timedelta as TimeDelta
# import logging
from multiprocessing import Process
from threading import Thread
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.API.koreaInvestment.Lib import kis_auth as ka, kis_domstk as kb

import pymysql
from Stock.LIB.RDB import pyMysqlConnector
from Stock.CONFIG import ConstTableName
from datetime import datetime, timedelta
from datetime import datetime as Datetime, timedelta , date
from Stock.LIB.Functions.Switch import StockSwitchTable

# from Stock.API.naver import stock_naver_get_Industry_category_1
# th1 = Thread(target=stock_naver_get_Industry_category_1.main)
# # th1.daemon = True
# th1.start()

# # # [010101] 네이버 산업 카테고리 조회
# from Stock.API.naver import stock_naver_get_industry_detail_2
# print("[Process START : " + strNowTime + "] stock_naver_get_industry_detail_2")
# th1 = Thread(target=stock_naver_get_industry_detail_2.main)
# # th1.daemon = True
# th1.start()
# print("[Process END : " + strNowTime + "] stock_naver_get_industry_detail_2")


dtNow = datetime.today()

strBaseYYYY = str(dtNow.year).zfill(4)
strBaseMM = str(dtNow.month).zfill(2)
strBaseDD = str(dtNow.day).zfill(2)
strBaseHH = str(dtNow.hour).zfill(2)
strBaseII = str(dtNow.minute).zfill(2)
strBaseSS = str(dtNow.second).zfill(2)

intBaseHH = int(strBaseHH)
intBaseII = int(strBaseII)


strBaseYYYYMM = strBaseYYYY + strBaseMM
strBaseYYYYMMDD = strBaseYYYYMM + strBaseDD


dBaseIssueDatetime = date(int(strBaseYYYY), int(strBaseMM), int(strBaseDD))

strNowDate = strBaseYYYY + strBaseMM + strBaseDD
strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS
strProcessType = '001000'

# # DB 연결
ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

sqlSelectCalendar = "SELECT * FROM " + ConstTableName.KoreaInvestOpenCalendarTable + " WHERE "
sqlSelectCalendar += " YYYYMMDD = %s "
cursorStockFriends.execute(sqlSelectCalendar, (strNowDate))

rstDBCalendarData = cursorStockFriends.fetchone()

strDBOpenFlag = str(rstDBCalendarData.get('open_flag'))

print("[Process START : " + strNowTime + "] strDBOpenFlag => " , type(strDBOpenFlag) , strDBOpenFlag)


dictUpdateData = dict()
dictUpdateData['result'] = '10'
dictUpdateData['data_1'] = str(strNowTime)
dictUpdateData['data_2'] = str(intBaseHH)
dictUpdateData['data_3'] = str(strDBOpenFlag)
StockSwitchTable.SwitchResultUpdateV4(strProcessType, dtNow, dictUpdateData)

if strBaseHH == "18":
    if strBaseII == "30":
        # [010002]달러/원 환율 수집
        from Stock.API.naver import stock_naver_exchange_rate_1
        print("[Process START : " + strNowTime + "] stock_naver_exchange_rate_1")
        th1 = Thread(target=stock_naver_exchange_rate_1.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] stock_naver_exchange_rate_1")


if strDBOpenFlag == 'Y':
    # 장 시작 전
    if strBaseHH == "05":

        if strBaseII == "00":
            # [010101] 네이버 산업 카테고리 조회

            print("[Process START : " + strNowTime + "] stock_naver_get_Industry_category_1")
            from Stock.API.naver import stock_naver_get_Industry_category_1
            th1 = Thread(target=stock_naver_get_Industry_category_1.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_naver_get_Industry_category_1")

        if strBaseII == "10":
            # [010201]거래중지 및 거래 유의 종목 업데이트
            from Stock.API.naver import stock_naver_update_stop_item_4

            print("[Process START : " + strNowTime + "] stock_naver_update_stop_item_4")
            th1 = Thread(target=stock_naver_update_stop_item_4.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_naver_update_stop_item_4")

        if strBaseII == "20":
            # [010202]관리종목 종목 업데이트
            from Stock.API.naver import stock_naver_update_manage_item_4
            print("[Process START : " + strNowTime + "] stock_naver_update_manage_item_4")
            th1 = Thread(target=stock_naver_update_manage_item_4.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_naver_update_manage_item_4")

        if strBaseII == "30":
            # [010203]네이버 경고 종목 업데이트
            from Stock.API.naver import stock_naver_update_alert_item_4
            print("[Process START : " + strNowTime + "] stock_naver_update_alert_item_4")
            th1 = Thread(target=stock_naver_update_alert_item_4.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_naver_update_alert_item_4")





        #장 마감 후
    if strBaseHH == "16":

        if strBaseII == "00":
            #[010102] 네이버 업종별 시세 조회
            from Stock.API.naver import stock_naver_get_industry_detail_2
            print("[Process START : " + strNowTime + "] stock_naver_get_industry_detail_2")
            th1 = Thread(target=stock_naver_get_industry_detail_2.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_naver_get_industry_detail_2")


        if strBaseII == "20":
            # [010103] 네이버 업종별 상세정보 조회
            from Stock.API.naver import stock_naver_get_item_detail_3
            print("[Process START : " + strNowTime + "] stock_naver_get_industry_detail_2")
            th1 = Thread(target=stock_naver_get_item_detail_3.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_naver_get_industry_detail_2")

        if strBaseII == "30":
            # [010001]코스피/코스닥 종합주가지수 수집
            from Stock.API.naver import stock_naver_get_stock_index
            print("[Process START : " + strNowTime + "] stock_naver_get_stock_index")
            th1 = Thread(target=stock_naver_get_stock_index.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_naver_get_stock_index")



        #장 마감 후
    if strBaseHH == "17":
        if strBaseII == "00":
            # [040101] 공공데이터 주가 수집
            from Stock.API.Government import stock_government_get_stock_data
            print("[Process START : " + strNowTime + "] stock_government_get_stock_data")
            # th1 = Thread(target=stock_government_get_stock_data.main)
            # # th1.daemon = True
            # th1.start()
            # print("[Process END : " + strNowTime + "] stock_government_get_stock_data")

    if strBaseHH == "18":
        if strBaseII == "00":
            # [020103] 한국투자 일일 주가 조회 naver item table 업데이트
            from Stock.API.koreaInvestment import stock_koreainvest_today_price_get_1
            print("[Process START : " + strNowTime + "] stock_koreainvest_today_price_get_1")
            th1 = Thread(target=stock_koreainvest_today_price_get_1.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_koreainvest_today_price_get_1")



        if strBaseII == "30":
            # [010002]달러/원 환율 수집
            from Stock.API.naver import stock_naver_get_stock_index
            print("[Process START : " + strNowTime + "] stock_naver_get_stock_index")
            th1 = Thread(target=stock_naver_get_stock_index.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_naver_get_stock_index")



        #장 마감 후
    if strBaseHH == "22":
        if strBaseII == "00":
            # [020102] 한국투자 주식 현재가격 조회
            from Stock.API.koreaInvestment import stock_koreainvest_now_price_get_1
            print("[Process START : " + strNowTime + "] stock_koreainvest_now_price_get_1")
            th1 = Thread(target=stock_koreainvest_now_price_get_1.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_koreainvest_now_price_get_1")






dictUpdateData = dict()
dictUpdateData['result'] = '00'
dictUpdateData['data_1'] = str(strNowTime)
dictUpdateData['data_2'] = str(strBaseHH)
dictUpdateData['data_3'] = str(strBaseII)
dictUpdateData['data_4'] = str(strDBOpenFlag)
StockSwitchTable.SwitchResultUpdateV4(strProcessType, dtNow, dictUpdateData)