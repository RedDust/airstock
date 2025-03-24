#챠트 5일선 20일선 분석 매매 프로그램
#5일선이 20일 선을 돌파 할때 매수
#5일선이 20일 선을 후퇴 할때 매도

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
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "aaaa333333333333")
        logging.info("["+ SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "]---!@#!@#!@#!@#!@#---------" + str(intLoop))

        strTestTable = 'stockfriends_koreainvest_recommendation_6'
        strItemCode = '108490'

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
        cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)


        sqlSelectItems = "SELECT * FROM " +ConstTableName.NaverStockItemTable +" WHERE state != '99' "
        sqlSelectItems += " AND market_code in ('KOSDAQ' , 'KOSPI', 'ETC') "
        sqlSelectItems += " AND sectors_code not in ( '277' , '280' , '25' ) "
        sqlSelectItems += " AND item_code = %s "
        sqlSelectItems += " ORDER BY seq ASC "
        sqlSelectItems += " LIMIT 1 "

        cursorStockFriends.execute(sqlSelectItems, (strItemCode))
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

                dictBeforeAverageValue = dict()



                sqlSelectItemTable = " SELECT * FROM " + strItemTableName + " ORDER BY YYYYMMDD ASC"
                sqlSelectItemTable += " LIMIT 121 "

                cursorStockFriends.execute(sqlSelectItemTable)
                rstLastItemDatas = cursorStockFriends.fetchall()

                for rstLastItemData in rstLastItemDatas:
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " ++++++++++++++++++++++++++++++++++++++++++++++ ")
                    LastItemDataSeq = str(rstLastItemData.get('seq'))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataSeq=> [" + str(
                        strDBSectorsName) + "][" + str(LastItemDataSeq) + "]")

                    LastItemDataPrice = str(round(rstLastItemData.get('now_price')))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataPrice=> [" + str(
                        strDBSectorsName) + "][" + str(LastItemDataPrice) + "]")

                    LastItemDataHHII = str(rstLastItemData.get('HHII'))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataHHII=> [" + str(
                        strDBSectorsName) + "][" + str(LastItemDataHHII) + "]")

                    LastItemDataYYYYMMDD = str(rstLastItemData.get('YYYYMMDD'))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataYYYYMMDD=> [" + str(
                        strDBSectorsName) + "][" + str(LastItemDataYYYYMMDD) + "]")

                    LastItemDataTradeVolume = str(rstLastItemData.get('trade_volume'))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataTradeVolume=> [" + str(
                        strDBSectorsName) + "][" + str(LastItemDataTradeVolume) + "]")

                    strAverageInfo = rstLastItemData.get('average_info')
                    jsonAverageInfos = json.loads(strAverageInfo)
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strAverageInfo=> [" + str(
                        strDBSectorsName) + "]["+str(type(jsonAverageInfos))+"]["+str(len(jsonAverageInfos))+"][" + str(strAverageInfo) + "]")

                    if len(jsonAverageInfos) != 6:
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " continue ")
                        continue

                    LastItemDataregDate = str(rstLastItemData.get('reg_date'))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataregDate=> [" + str(strDBSectorsName) + "][" + str(LastItemDataregDate) + "]")

                    # jsonAverageInfos['5']
                    # jsonAverageInfos['10']
                    # jsonAverageInfos['20']
                    # jsonAverageInfos['30']
                    # jsonAverageInfos['60']
                    # jsonAverageInfos['120']

                    # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " jsonAverageInfos['120']=> [" + str(
                    #     jsonAverageInfos['120']) + "][" + str(len(
                    #     jsonAverageInfos['120'])) + "][" + str(jsonAverageInfos['120']) + "]")

                    if len(jsonAverageInfos['120']) < 1:
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " continue ")
                        continue


                    for jsonAverageInfoKey,jsonAverageInfoValue  in jsonAverageInfos.items():
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " ------------------------------------ ")
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " jsonAverageInfos=> [" + str(
                            jsonAverageInfoKey) + "][" + str(jsonAverageInfoValue) + "]")

                        intNowAvgVolume = str(jsonAverageInfoValue.get('avg_volume'))
                        intNowavgPrice = str(jsonAverageInfoValue.get('avg_price'))

                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " jsonAverageInfoValue.get=> ["+str(jsonAverageInfoKey)+"][" + str(intNowAvgVolume) + "]["+intNowavgPrice+"]["+LastItemDataPrice+"]")
                        # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " LastItemDataPrice=> ["+str(strDBSectorsName)+"][" + str(LastItemDataPrice) + "]")






                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " 0000000000000000000000000000000000000000000000000000 AFTER ")

                        dictBeforeAverageValue = jsonAverageInfos









        # cursorStockFriends.execute()
        # ResStockFriendsConnection.commit()

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
