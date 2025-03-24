import sys, os
import time

sys.path.append("D:/PythonProjects/airstock")

import urllib.request, requests
import multiprocessing as mp
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
from threading import Thread
from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName
from Stock.LIB.SeleniumModule.Windows import Chrome, Firefox
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Lib.CustomException.QuitException import QuitException
import inspect as Isp, logging, logging.handlers
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Functions.Switch import StockSwitchTable
from Stock.LIB.Functions.Table import ManufactureItemTable as MIT
import Stock.API.koreaInvestment.stock_koreainvest_get_price_slave as GetPrice

import Stock.API.koreaInvestment.stock_deamon_test as StockDeamonTest



import multiprocessing as mp





def main():

    try:

        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Process START]======")

        # 초기값
        strProcessType = '000001'
        strDBSequence = '0'
        strDBSectorsName = ''
        intItemLoop = 0

        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = 'Stock/' + strAddLogPath + '/' + strProcessType

        dtNow = DateTime.today()
        setLogger = ULF.setLogFileV2(logging, LogPath)

        intLoop = 0

        # while True:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "--->" + str(intLoop) + " START ")

        # logging.info("[" + SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "]---------------------------------" + str(intLoop) + " => " + str(bbbb))
        # slave_thread():
        # th1 = Thread(target=StockDeamonTest.main, args=(str(intLoop)))

        listProcess = []

        th1 = multiprocessing.Process(name="Sub Process", target=StockDeamonTest.main, args=(str(intLoop)), daemon=True)

        logging.info("[" + SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "]---------------------------------" + str(intLoop) + " => " + str(th1))
        # th1.daemon = True
        th1.start()
        listProcess.append(th1)

        logging.info("[" + SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "]---------------------------------" + str(
            intLoop) + " => " + str(th1))
        # th1.join()

        time.sleep(3)
        logging.info("[" + SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "]---------------------------------" + str(
            intLoop) + " => " + str(th1))

        for p in listProcess:
            p.join()  # 프로세스가 모두 종료될 때까지 대기

        intLoop += 1



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
                              Isp.currentframe()) + "[Finally END]======")



if __name__ == '__main__':
    main()
