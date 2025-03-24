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
from urllib import parse
from Realty.Government.Init import init_conf
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup
from multiprocessing import Process
import multiprocessing
from random import choice, random

from threading import Thread
from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName
from Stock.LIB.SeleniumModule.Windows import Chrome, Firefox
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Lib.CustomException.QuitException import QuitException
from Stock.LIB.Logging import MultiLogClass as MLC
from Stock.LIB.Functions.Switch import StockSwitchTable
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.LIB.Functions.Table import ManufactureItemTable as MIT

import Stock.API.koreaInvestment.Lib.kis_auth as ka
import Stock.API.koreaInvestment.Lib.kis_domstk as kb
import Stock.API.koreaInvestment.stock_koreainvest_get_price_slave as GetPrice

def main():

    try:

        # 초기값
        strProcessType = '020204'
        strDBSequence = '0'
        strDBSectorsName = ''
        intItemLoop = intLoop = 0
        strItemDatabaseName='stockfriends'

        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = 'Stock/' + strAddLogPath + '/' + strProcessType

        intLoop = 0

        dtNow = DateTime.today()
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

        strTestTable = 'stockfriends_koreainvest_recommendation_6'


        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
        cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)


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

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " ====================================== ")

                strDBSequence = rstSelectData.get('seq')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSequence=> ["+str(intItemLoop)+"][" + str(strDBSequence) + "]")

                strDBSectorsName = rstSelectData.get('item_name')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSectorsName=> ["+str(intItemLoop)+"][" + str(strDBSectorsName) + "]")

                strDBItemCode = rstSelectData.get('item_code')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(strDBSectorsName) + "][" + str(strDBItemCode) + "]")

                strAverageInfo = rstSelectData.get('average_info')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strAverageInfo=> [" + str(strDBSectorsName) + "][" + str(strAverageInfo) + "]")

                jsonAverageInfos = json.loads(strAverageInfo)

                strDBCountryCode = str(rstSelectData.get('country_code'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBCountryCode=> [" + str(strDBSectorsName) + "][" + str(strDBCountryCode) + "]")

                strDBUnitPrice = str(rstSelectData.get('unit_price'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBUnitPrice=> [" + str(strDBSectorsName) + "][" + str(strDBUnitPrice) + "]")

                strDBMultiplication = str(rstSelectData.get('multiplication'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBMultiplication=> [" + str(strDBSectorsName) + "][" + str(strDBMultiplication) + "]")

                strDBState = str(rstSelectData.get('state'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBState=> [" + str(strDBSectorsName) + "][" + str(strDBState) + "]")

                strItemTableName = MIT.GetItemTableName(strDBItemCode, strDBCountryCode, strItemDatabaseName)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemTableName=> [" + str(len(
                    strItemTableName)) + "][" + str(
                    strItemTableName) + "]")

                sqlSelectItemTable = "SELECT * FROM " + strItemTableName + " ORDER BY YYYYMMDD ASC "
                cursorStockFriends.execute(sqlSelectItemTable)
                rstLastItemDatas = cursorStockFriends.fetchall()

                for rstLastItemData in rstLastItemDatas:
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " ------------------------ ")

                    LastItemDataSeq = str(rstLastItemData.get('seq'))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataSeq=> [" + str(
                        strDBSectorsName) + "][" + str(LastItemDataSeq) + "]")

                    LastItemDataPrice = str(rstLastItemData.get('now_price'))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataPrice=> [" + str(
                        strDBSectorsName) + "][" + str(LastItemDataPrice) + "]")

                    LastItemDataYYYYMMDD = str(rstLastItemData.get('YYYYMMDD'))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataYYYYMMDD=> [" + str(
                        strDBSectorsName) + "][" + str(LastItemDataYYYYMMDD) + "]")

                    LastItemDataTradeVolume = str(rstLastItemData.get('trade_volume'))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataTradeVolume=> [" + str(
                        strDBSectorsName) + "][" + str(LastItemDataTradeVolume) + "]")

                    listAverage = []
                    listAverage.append('5')
                    listAverage.append('10')
                    listAverage.append('20')
                    listAverage.append('30')
                    listAverage.append('60')
                    listAverage.append('120')

                    dictAverage = dict()
                    for strAverage in listAverage:
                        dictAverage[strAverage] = dict()
                        intAverage = int(strAverage)

                        sqlSelectAverage = " SELECT round(avg(now_price)) as avg_price , "
                        sqlSelectAverage += " MAX(now_price) as max_price , "
                        sqlSelectAverage += " MIN(now_price) as min_price , "
                        sqlSelectAverage += " round(avg(trade_volume)) as avg_volume , "
                        sqlSelectAverage += " MAX(trade_volume) as max_volume , "
                        sqlSelectAverage += " MIN(trade_volume) as min_volume , "
                        sqlSelectAverage += " count(*) as cnt  "
                        sqlSelectAverage += " FROM (select now_price , trade_volume from "+strItemTableName+" WHERE state='00' "
                        sqlSelectAverage += " AND YYYYMMDD <= %s "
                        sqlSelectAverage += " ORDER BY YYYYMMDD DESC LIMIT "+strAverage+" ) as sub "

                        logging.info(
                            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " sqlSelectAverage=> [" + str(
                                strDBSectorsName) + "][" + str(sqlSelectAverage) + "]["+str(LastItemDataYYYYMMDD)+"]")

                        cursorStockFriends.execute(sqlSelectAverage, (LastItemDataYYYYMMDD))
                        rstSelectAverage = cursorStockFriends.fetchone()
                        strDBCount = rstSelectAverage.get('cnt')
                        intDBCount = int(strDBCount)
                        if intDBCount != intAverage:
                            continue

                        dictAverage[strAverage]['avg_price'] = str(round(rstSelectAverage.get('avg_price')))
                        dictAverage[strAverage]['max_price'] = str(round(rstSelectAverage.get('max_price')))
                        dictAverage[strAverage]['min_price'] = str(round(rstSelectAverage.get('min_price')))
                        dictAverage[strAverage]['avg_volume'] = str(round(rstSelectAverage.get('avg_volume')))
                        dictAverage[strAverage]['max_volume'] = str(round(rstSelectAverage.get('max_volume')))
                        dictAverage[strAverage]['min_volume'] = str(round(rstSelectAverage.get('min_volume')))

                    strJsonAverage = json.dumps(dictAverage)
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strJsonAverage=> [" + str(
                        strDBSectorsName) + "][" + str(strJsonAverage) + "]")

                    sqlUpdateNaverItemLists = "UPDATE " + strItemTableName + " SET average_info = %s "
                    sqlUpdateNaverItemLists += " WHERE seq = %s "

                    print("sqlUpdateNaverItemLists ==> ", sqlUpdateNaverItemLists)
                    print("strJsonAverage ==> ", strJsonAverage)
                    print("LastItemDataSeq ==> ", LastItemDataSeq)

                    cursorStockFriends.execute(sqlUpdateNaverItemLists, (strJsonAverage , LastItemDataSeq))
                    ResStockFriendsConnection.commit()

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = LastItemDataYYYYMMDD
                dictSwitchData['data_2'] = strDBSequence
                dictSwitchData['data_3'] = strDBSectorsName
                dictSwitchData['data_4'] = intItemLoop
                StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'b', dictSwitchData)
                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "============================time.sleep(1) ")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " ============================time.sleep(1)")

                intItemLoop += 1

                time.sleep(1)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

        print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "=======================================")

        return "FAIL"


    except Exception as e:

        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Error Exception]========================================================")

        print("Exception e=>" , e)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(e))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(e)))
        err_msg = str(traceback.format_exc())
        print("Exception err_msg=>", err_msg)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(err_msg))

    else:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[ELSE]========================================================")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]========================================================")

if __name__ == '__main__':
    main()
