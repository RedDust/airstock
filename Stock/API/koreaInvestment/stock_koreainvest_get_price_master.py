# 금융위원회_주식시세정보
# 2024-11-25 커밋
#https://www.data.go.kr/iim/api/selectAPIAcountView.do
#https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo

import sys, os
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
import multiprocessing
from threading import Thread
from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName

from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Lib.CustomException.QuitException import QuitException


def main():

    from Stock.LIB.Functions.Switch import StockSwitchTable
    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
    from Stock.LIB.Functions.Table import ManufactureItemTable as MIT
    import Stock.API.koreaInvestment.Lib.kis_auth as ka
    import Stock.API.koreaInvestment.Lib.kis_domstk as kb
    import Stock.API.koreaInvestment.stock_koreainvest_get_price_slave as GetPrice
    import Stock.API.koreaInvestment.stock_koreainvest_now_recommendation_4_slave as RecommendSlave


    import Stock.API.koreaInvestment.stock_deamon_test as StockDeamonTest

    # 토큰 발급
    ka.auth()

    try:

        dtNow = DateTime.today()

        strBaseYYYY = str(dtNow.year).zfill(4)
        strBaseMM = str(dtNow.month).zfill(2)
        strBaseDD = str(dtNow.day).zfill(2)
        strBaseHH = str(dtNow.hour).zfill(2)
        strBaseII = str(dtNow.minute).zfill(2)
        strBaseSS = str(dtNow.second).zfill(2)

        strItemDatabaseName='koreainvest'

        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

        strNowYYYYMMDD = strBaseYYYY + strBaseMM + strBaseDD
        strNowHHMM = strBaseHH + strBaseII

        # if strBaseII == '10':

        # 초기값
        strProcessType = '020110'
        strSlaveProcessType = '020111'
        strDBSequence = '0'
        strDBSectorsName = ''
        intItemLoop = 0

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
        dictSwitchData['int_data_1'] = '0'
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'a', dictSwitchData)

        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['int_data_1'] = '0'
        StockSwitchTable.SwitchResultUpdateV2(logging, strSlaveProcessType, 'a', dictSwitchData)

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
        # cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)

        sqlSelectNaverItemLists = " SELECT * FROM " +ConstTableName.NaverStockItemTable +" WHERE state != '99' "
        sqlSelectNaverItemLists += " AND sectors_code not in ( '277' , '280' , '25' )"
        sqlSelectNaverItemLists += " AND market_code in ('KOSDAQ' , 'KOSPI', 'ETC')"
        sqlSelectNaverItemLists += " ORDER BY market_cap DESC "
        # sqlSelectNaverItemLists += " LIMIT 10 "

        cursorStockFriends.execute(sqlSelectNaverItemLists)
        intAffectedCount = cursorStockFriends.rowcount
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                intAffectedCount) + "]")


        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
            intAffectedCount) + "]")

        rstSelectDatas = cursorStockFriends.fetchall()
        if rstSelectDatas == None:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


        # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstSelectDatas >> " + str(rstSelectDatas))

        procs = []
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[GET Price START : =====================================]")
        intItemLoop = 0
        for rstSelectData in rstSelectDatas:
            strDBSequence = str(int(rstSelectData.get('seq')))
            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSequence=> ["+str(intItemLoop)+"][" + str(strDBSequence) + "]")

            # strDBSectorsName = rstSelectData.get('item_name')
            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSectorsName=> ["+str(intItemLoop)+"][" + str(strDBSectorsName) + "]")
            #
            strDBItemCode = rstSelectData.get('item_code')
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(intItemLoop) + "][" + str(strDBItemCode) + "]")

            strDBCountryCode = str(rstSelectData.get('country_code'))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBCountryCode=> [" + str(intItemLoop) + "][" + str(strDBCountryCode) + "]")

            # strDBUnitPrice = str(int(rstSelectData.get('unit_price')))
            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBUnitPrice=> [" + str(intItemLoop) + "][" + str(strDBUnitPrice) + "]")
            #
            # strDBMultiplication = str(int(rstSelectData.get('multiplication')))
            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBMultiplication=> [" + str(intItemLoop) + "][" + str(strDBMultiplication) + "]")
            #
            # strMarketCap = str(int(rstSelectData.get('market_cap')))
            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strMarketCap=> [" + str(intItemLoop) + "][" + str(strMarketCap) + "]")
            #
            # strMarketVolume = str(int(rstSelectData.get('market_volume')))
            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strMarketVolume=> [" + str(intItemLoop) + "][" + str(strMarketVolume) + "]")
            #
            # strDBState = str(rstSelectData.get('state'))
            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBState=> [" + str(intItemLoop) + "][" + str(strDBState) + "]")


            strItemTableName = MIT.GetItemTableName(strDBItemCode, strDBCountryCode, strItemDatabaseName)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemTableName=> [" + str(len(
                strItemTableName)) + "][" + str(
                strItemTableName) + "]")

            if len(strItemTableName) < 1:
                raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

            # # 테이블 없으면 제작
            # bCreateResult = MIT.CheckExistItemTable(strItemTableName, KoreaInvestItemsConnection,
            #                                         strItemDatabaseName)
            #
            # if bCreateResult != True:
            #     raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

            if (intItemLoop % 18) == 0:
                time.sleep(1)

            th1 = Thread(target=GetPrice.child_thread, args=(logging, StockSwitchTable, strDBItemCode, dtNow, strItemTableName))

            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " th1=> [" + str(intItemLoop) + "][" + str(th1) + "]")

            # th1.daemon = True
            th1.start()
            # th1.join()

            if (intItemLoop % 100) == 0:
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + "[GET Price Prccess : =====================================]")


            intItemLoop +=1

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[GET Price END : =====================================]")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # 추가하면 5분 지나감
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_2'] = strDBSequence
        dictSwitchData['data_3'] = strDBSectorsName
        dictSwitchData['data_4'] = intItemLoop
        dictSwitchData['data_5'] = intAffectedCount
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'b', dictSwitchData)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[GET RECOMMEND START : =====================================]")
        for rstSelectData in rstSelectDatas:
            strDBSequence = rstSelectData.get('seq')
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSequence=> [" + str(intItemLoop) + "][" + str(
                    strDBSequence) + "]")

            strDBSectorsName = rstSelectData.get('item_name')
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSectorsName=> [" + str(
                intItemLoop) + "][" + str(strDBSectorsName) + "]")

            strDBItemCode = rstSelectData.get('item_code')
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(intItemLoop) + "][" + str(
                    strDBItemCode) + "]")

            strDBCountryCode = str(rstSelectData.get('country_code'))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBCountryCode=> [" + str(
                intItemLoop) + "][" + str(strDBCountryCode) + "]")

            strDBUnitPrice = str(rstSelectData.get('unit_price'))
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBUnitPrice=> [" + str(intItemLoop) + "][" + str(
                    strDBUnitPrice) + "]")

            strDBMultiplication = str(rstSelectData.get('multiplication'))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBMultiplication=> [" + str(
                intItemLoop) + "][" + str(strDBMultiplication) + "]")

            strDBState = str(rstSelectData.get('state'))
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBState=> [" + str(intItemLoop) + "][" + str(
                    strDBState) + "]")

            strItemTableName = MIT.GetItemTableName(strDBItemCode, strDBCountryCode, strItemDatabaseName)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemTableName=> [" + str(len(
                strItemTableName)) + "][" + str(
                strItemTableName) + "]")

            if len(strItemTableName) < 1:
                raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

            dtNowStartTime = dtNow - TimeDelta(minutes=5)
            dtNowEndTime = dtNow

            if int(dtNowEndTime.hour >= 10):
                th1 = Thread(target=RecommendSlave.slave_thread, args=(logging, dtNowStartTime, dtNowEndTime, rstSelectData, strItemTableName))
                # th1.daemon = True
                th1.start()


        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[GET RECOMMEND END : =====================================]")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = strNowDate
        dictSwitchData['data_2'] = strDBSequence
        dictSwitchData['data_3'] = strDBSectorsName
        dictSwitchData['data_4'] = intItemLoop
        dictSwitchData['data_5'] = intAffectedCount
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Error QuitException")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(e))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
        err_msg = str(traceback.format_exc())
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))


    except Exception as e:

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
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[ELSE]========================================================")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]========================================================")


if __name__ == '__main__':
    main()