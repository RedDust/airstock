#KoreaInvestGetData
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
strProcessType = '002000'

strBaseYYYYMM = strBaseYYYY + strBaseMM
strBaseYYYYMMDD = strBaseYYYYMM + strBaseDD


dBaseIssueDatetime = date(int(strBaseYYYY), int(strBaseMM), int(strBaseDD))

strNowDate = strBaseYYYY + strBaseMM + strBaseDD
strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS


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
from Stock.LIB.Functions.Switch import StockSwitchTable
StockSwitchTable.SwitchResultUpdateV4(strProcessType, dtNow, dictUpdateData)

if strBaseHH == "00":
    if strBaseII == "00":

        #[020101] 한국투자 주식 거래일 조회
        from Stock.API.koreaInvestment import stock_koreainvest_service_open_info_calendar_get_1
        print("[Process START : " + strNowTime + "] stock_koreainvest_service_open_info_calendar_get_1")
        th1 = Thread(target=stock_koreainvest_service_open_info_calendar_get_1.main)
        # th1.daemon = True
        th1.start()
        print("[Process END : " + strNowTime + "] stock_koreainvest_service_open_info_calendar_get_1")



if strDBOpenFlag == 'Y':
    # 장 중

    if intBaseHH >= 9 and intBaseHH <= 15:

        if intBaseHH != 15 or intBaseII < 20:
            # 15시 10분 까지 가져 온다.
            print("[Process START : " + strNowTime + "] intBaseHH == 15 and strBaseII <= 30")
            # # [020102] 한국투자 주식 현재가격 조회
            from Stock.API.koreaInvestment import stock_koreainvest_get_price_master
            print("[Process START : " + strNowTime + "] stock_koreainvest_get_price_master")
            th1 = Thread(target=stock_koreainvest_get_price_master.main)
            # th1.daemon = True
            th1.start()
            print("[Process END : " + strNowTime + "] stock_koreainvest_get_price_master")

        else:
            print("[Process START : " + strNowTime + "] intBaseHH >= 9 and intBaseHH <= 15")

    if intBaseHH == 15 and intBaseII == 10:
        print("[Process START : " + strNowTime + "] intBaseHH == 15 and strBaseII <= 30")

    if intBaseHH == 15 and intBaseII == 15:
        print("[Process START : " + strNowTime + "] intBaseHH == 15 and strBaseII <= 30")





dictUpdateData = dict()
dictUpdateData['result'] = '00'
dictUpdateData['data_1'] = str(strNowTime)
dictUpdateData['data_2'] = str(intBaseHH)
dictUpdateData['data_3'] = str(strDBOpenFlag)
from Stock.LIB.Functions.Switch import StockSwitchTable
StockSwitchTable.SwitchResultUpdateV4(strProcessType, dtNow, dictUpdateData)
