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

def main(intLoop):

    try:

        print('SLAVE finished:===>', intLoop)

        time.sleep(5)

        # 초기값
        strProcessType = '000002'
        strDBSequence = '0'
        strDBSectorsName = ''
        intItemLoop = intLoop

        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = 'Stock/' + strAddLogPath + '/' + strProcessType

        intLoop = 0

        dtNow = DateTime.today()
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "aaaa333333333333")
        logging.info("["+ SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "]---!@#!@#!@#!@#!@#---------" + str(intLoop))

        strTestTable = 'stockfriends_koreainvest_recommendation_temp_test'

        sqlInsertTest = "INSERT INTO " + strTestTable + " SET "
        sqlInsertTest += "average_info = %s "

        dictJson = dict()
        dictJson['5'] = 1200
        dictJson['10'] = 1300
        dictJson['20'] = 1400
        dictJson['60'] = 1500
        dictJson['120'] = 1600

        strJSON = json.dumps(dictJson)
        print("strJSON=>", type(strJSON), strJSON)

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        cursorStockFriends.execute(sqlInsertTest, (strJSON))
        ResStockFriendsConnection.commit()

        print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) , "=======================================")

        print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) , "affffffffffffffffaaaaa")
        #

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
