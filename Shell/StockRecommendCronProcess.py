#KoreaInvestGetData

# 종목 추천 프로그램인데
# stock_koreainvest_get_price_master.py
# 에 추가 되었다.
# 추가되면서

import os
import sys
sys.path.append("D:/PythonProjects/airstock")

import subprocess
import time
from datetime import datetime as DateTime, timedelta as TimeDelta
# import logging
from multiprocessing import Process
import multiprocessing

from threading import Thread
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.API.koreaInvestment.Lib import kis_auth as ka, kis_domstk as kb

import pymysql
from Stock.LIB.RDB import pyMysqlConnector
from Stock.CONFIG import ConstTableName
from datetime import datetime as Datetime, timedelta , date

import inspect as Isp, logging, logging.handlers
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Init.Functions.Logs import GetLogDef as SLog

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


dtNow = Datetime.today()

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


# # DB 연결
ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

sqlSelectCalendar = "SELECT * FROM " + ConstTableName.KoreaInvestOpenCalendarTable + " WHERE "
sqlSelectCalendar += " YYYYMMDD = %s "
cursorStockFriends.execute(sqlSelectCalendar, (strNowDate))

rstDBCalendarData = cursorStockFriends.fetchone()

strDBOpenFlag = str(rstDBCalendarData.get('open_flag'))

print("[Process START : " + strNowTime + "] strDBOpenFlag => " , type(strDBOpenFlag) , strDBOpenFlag)




def main():

    strProcessType = '003000'
    strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
    LogPath = 'Stock/' + strAddLogPath + '/' + strProcessType

    dtNow = DateTime.today()
    setLogger = ULF.setLogFileV2(logging, LogPath)

    logging.info(SLog.Ins(Isp.getframeinfo,Isp.currentframe()) + "[Process START]======")


    dictUpdateData = dict()
    dictUpdateData['result'] = '10'
    dictUpdateData['data_1'] = str(strNowTime)
    dictUpdateData['data_2'] = str(intBaseHH)
    dictUpdateData['data_3'] = str(strDBOpenFlag)
    from Stock.LIB.Functions.Switch import StockSwitchTable
    StockSwitchTable.SwitchResultUpdateV4(strProcessType, dtNow, dictUpdateData)


    intLoop = 0
    listProcess = []
    if strDBOpenFlag == 'Y':
        # 장 중
        if intBaseHH >= 9 and intBaseHH <= 15:

            if intBaseHH != 15 or intBaseII <= 30:
                # 15시 30분 까지 가져 온다.

                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+"[Process START : " + strNowTime + "] intBaseHH == 15 and strBaseII <= 30")
                # # [020102] 한국투자 주식 현재가격 조회
                # import Stock.API.koreaInvestment.stock_deamon_test as StockDeamonTest
                # th1 = multiprocessing.Process(name="Sub Process", target=StockDeamonTest.main, args=(str(intLoop)),
                #                               daemon=True)
                # from Stock.API.koreaInvestment import stock_koreainvest_now_recommendation_4_master
                # th1 = multiprocessing.Process(name="stock_koreainvest_now_recommendation_4_master", target=stock_koreainvest_now_recommendation_4_master.main)
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "->" + str(intLoop) + " => " + str(th1))
                # th1.daemon = True
                # th1.start()
                # listProcess.append(th1)
                # # print("[Process START : " + strNowTime + "] stock_koreainvest_now_recommendation_4_master")
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+"[Process intBaseHH != 15 or intBaseII <= 30 END : " + strNowTime + "] stock_koreainvest_now_recommendation_4_master")

            else:
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+"[Process ELSE END : " + strNowTime + "] intBaseHH >= 9 and intBaseHH <= 15")


        if intBaseHH == 15:
            if intBaseII == 15:
                print(SLog.Ins(Isp.getframeinfo,Isp.currentframe()) + "[Process START : " + strNowTime + "] intBaseHH == 15 and intBaseHH <= 30")
                from Stock.API.koreaInvestment import stock_koreainvest_now_recommendation_3
                th1 = multiprocessing.Process(name="stock_koreainvest_now_recommendation_3", target=stock_koreainvest_now_recommendation_3.main)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "->" + str(intLoop) + " => " + str(th1))
                th1.daemon = True
                th1.start()
                listProcess.append(th1)


        for p in listProcess:
            p.join()  # 프로세스가 모두 종료될 때까지 대기

    logging.info(SLog.Ins(Isp.getframeinfo,
                   Isp.currentframe()) + "[Process PERFECT END : " + strNowTime + "] ")

    dictUpdateData = dict()
    dictUpdateData['result'] = '00'
    dictUpdateData['data_1'] = str(strNowTime)
    dictUpdateData['data_2'] = str(intBaseHH)
    dictUpdateData['data_3'] = str(strDBOpenFlag)
    from Stock.LIB.Functions.Switch import StockSwitchTable
    StockSwitchTable.SwitchResultUpdateV4(strProcessType, dtNow, dictUpdateData)



if __name__ == '__main__':
    main()
