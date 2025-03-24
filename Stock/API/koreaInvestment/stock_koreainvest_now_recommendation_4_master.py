# 금융위원회_주식시세정보
# 2024-11-25 커밋
#https://www.data.go.kr/iim/api/selectAPIAcountView.do
#https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo

import sys ,os
import time

sys.path.append("D:/PythonProjects/airstock")

import urllib.request, requests
import json
import pymysql
import traceback
import xml
import xml.etree.ElementTree as ET
import pandas as pd

from Realty.Government.Init import init_conf
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName
from threading import Thread

from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Lib.CustomException.QuitException import QuitException

def main():

    print("ggggggggggggggggggggggggggggggggg")

    from Stock.LIB.Functions.Switch import StockSwitchTable
    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
    from Stock.LIB.Functions.Table import ManufactureItemTable as MIT
    import Stock.API.koreaInvestment.Lib.kis_auth as ka
    import Stock.API.koreaInvestment.Lib.kis_domstk as kb
    import Stock.API.koreaInvestment.stock_koreainvest_now_recommendation_4_slave as GetPrice

    # # 토큰 발급
    # ka.auth()

    try:
        print("---------------")
        dtNow = DateTime.today()
        dtNowTime = DateTime.today()

        strBaseYYYY = str(dtNow.year).zfill(4)
        strBaseMM = str(dtNow.month).zfill(2)
        strBaseDD = str(dtNow.day).zfill(2)
        strBaseHH = str(dtNow.hour).zfill(2)
        strBaseII = str(dtNow.minute).zfill(2)
        strBaseSS = str(dtNow.second).zfill(2)

        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS
        strNowYYYYMMDD = strNowDate
        strNowHHMM = strBaseHH + strBaseII

        # strNowYYYYMMDD = strNowDate = "20241212"

        #초기값
        strProcessType = '020203'
        strDBSequence = '0'
        strDBSectorsName = ''
        intItemLoop = 0
        strItemDatabaseName='koreainvest'

        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = 'Stock/'+strAddLogPath+'/'+ strProcessType

        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[CRONTAB START : =====================================]")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = StockSwitchTable.SwitchResultSelectV2(logging, strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        if strResult == '10':
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        if strResult == '20':
            strDBSequence = str(rstResult.get('data_2'))

        if strResult == '40':
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'a', dictSwitchData)

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)


        KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
        cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)

        listTimeDatas = []
        #        strNowHHMM = strBaseHH + strBaseII
        intLoopBaseHH = int(strBaseHH)
        intLoopBaseII = int(strBaseII)
        intNowLoopHHMM = int(strNowHHMM)

        intSetMinute = 0
        dtNowEndTime = dtNowTime

        # dtNowEndTime = DateTime(2024,12,18,15,10,0)

        if int(dtNowEndTime.hour) < 9 or int(dtNowEndTime.hour) > 15:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        elif int(dtNowEndTime.hour) == 15 and int(dtNowEndTime.minute) > 30:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        intModifyMinute = int(dtNowEndTime.minute) % 5

        if intModifyMinute!= 0:
            dtNowEndTime = dtNowEndTime - TimeDelta(minutes=intModifyMinute)

        dtNowStartTime = dtNowEndTime - TimeDelta(minutes=35)
        dtNowEndTime = dtNowEndTime - TimeDelta(minutes=5)

        dtNowStartTime = dtNowEndTime - TimeDelta(minutes=35)
        dtNowEndTime = dtNowEndTime - TimeDelta(minutes=5)


        # if int(dtNowStartTime.hour) < 9:
        #     raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴
        #
        # elif int(dtNowStartTime.hour) == 9 and int(dtNowStartTime.minute) == 0:
        #     raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


        strStartBaseHH = str(dtNowStartTime.hour).zfill(2)
        strStartBaseII = str(dtNowStartTime.minute).zfill(2)

        strEndBaseHH = str(dtNowEndTime.hour).zfill(2)
        strEndBaseII = str(dtNowEndTime.minute).zfill(2)

        strStartHHII = strStartBaseHH + strStartBaseII
        strEndHHII = strEndBaseHH + strEndBaseII
        listTimeDatas.append([strStartHHII, strEndHHII])

        print("dtNowStartTime => ", strStartBaseHH+strStartBaseII)
        print("dtNowEndTime => ", strEndBaseHH+strEndBaseII)
        print("154listTimeDatas => ", listTimeDatas)


        sqlSelectItems = "SELECT * FROM " +ConstTableName.NaverStockItemTable +" WHERE state != '99' "
        sqlSelectItems += " AND seq > %s "
        sqlSelectItems += " AND market_code in ('KOSDAQ' , 'KOSPI', 'ETC') "
        sqlSelectItems += " AND sectors_code not in ( '277' , '280' , '25' ) "
        sqlSelectItems += " ORDER BY seq ASC "
        # sqlSelectItems += " LIMIT 1 "

        cursorStockFriends.execute(sqlSelectItems, (strDBSequence))
        intAffectedCount = cursorStockFriends.rowcount
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                intAffectedCount) + "]")

        if intAffectedCount > 0:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                intAffectedCount) + "]")

            rstSelectDatas = cursorStockFriends.fetchall()
            if rstSelectDatas == None:
                raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstSelectDatas >> " + str(rstSelectDatas))
            for rstSelectData in rstSelectDatas:
                strDBSequence = rstSelectData.get('seq')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSequence=> ["+str(intItemLoop)+"][" + str(strDBSequence) + "]")

                strDBSectorsName = rstSelectData.get('item_name')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSectorsName=> ["+str(intItemLoop)+"][" + str(strDBSectorsName) + "]")

                strDBItemCode = rstSelectData.get('item_code')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(intItemLoop) + "][" + str(strDBItemCode) + "]")

                strDBCountryCode = str(rstSelectData.get('country_code'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBCountryCode=> [" + str(intItemLoop) + "][" + str(strDBCountryCode) + "]")

                strDBUnitPrice = str(rstSelectData.get('unit_price'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBUnitPrice=> [" + str(intItemLoop) + "][" + str(strDBUnitPrice) + "]")

                strDBMultiplication = str(rstSelectData.get('multiplication'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBMultiplication=> [" + str(intItemLoop) + "][" + str(strDBMultiplication) + "]")

                strDBState = str(rstSelectData.get('state'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBState=> [" + str(intItemLoop) + "][" + str(strDBState) + "]")

                strItemTableName = MIT.GetItemTableName(strDBItemCode, strDBCountryCode, strItemDatabaseName)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemTableName=> [" + str(len(
                    strItemTableName)) + "][" + str(
                    strItemTableName) + "]")

                if len(strItemTableName) < 1:
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                print("238listTimeDatas => " , len(listTimeDatas) , listTimeDatas)

                intListCount = len(listTimeDatas)

                intTimeLoop = 0
                for listTimeDataKey in listTimeDatas:

                    print("listTimeDataKey0 => ", intTimeLoop, listTimeDatas[intTimeLoop][0], listTimeDataKey)
                    print("listTimeDataKey1 => ", intTimeLoop, listTimeDatas[intTimeLoop][1], listTimeDataKey)

                    strStartHHMM = listTimeDatas[intTimeLoop][0]
                    strEndHHMM = listTimeDatas[intTimeLoop][1]

                    intTimeLoop += 1
                    # slave_thread():
                    th1 = Thread(target=GetPrice.slave_thread, args=(logging, dtNowStartTime, dtNowEndTime, rstSelectData, strItemTableName))
                    # th1.daemon = True
                    th1.start()

                    print("[316]=====================================================================================")



                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = strNowDate
                dictSwitchData['data_2'] = strDBSequence
                dictSwitchData['data_3'] = strDBSectorsName
                dictSwitchData['data_4'] = intItemLoop
                StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'b', dictSwitchData)


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

    except QuitException as e:

        print("--------")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Error QuitException")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(e))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
        err_msg = str(traceback.format_exc())
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

    except Exception as e:
        print("[Exception]--------")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Error Exception")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(e))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
        err_msg = str(traceback.format_exc())
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        dictSwitchData['data_1'] = strNowDate
        dictSwitchData['data_2'] = strDBSequence
        dictSwitchData['data_3'] = strDBSectorsName
        dictSwitchData['data_4'] = intItemLoop
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

    else:
        print("[else]--------")
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[ELSE]========================================================")

    finally:
        print("[finally]--------")
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]========================================================")


# if __name__ == '__main__':
#     main()