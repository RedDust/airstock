#경매 정보 도시구동 Select 가져 오기
#cURL 동작 안해서 실패함 시간 아까움



import requests

# This is a sample Python script.
import sys
import json
import os
import subprocess
import time
import random
import pymysql
sys.path.append("D:/PythonProjects/airstock")
import logging
import logging.handlers
import traceback


#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Realty.Auction.Const import ConstRealEstateTable_AUC
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Realty.Auction.Const import AuctionCourtInfo
from Init.DefConstant import ConstRealEstateTable
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.CryptoModule import AesCrypto
from Realty.Auction.AuctionLib import MakeAuctionUniqueKey
from Lib.Logging import Logging
from Init.Functions.Logs import GetLogDef as SLog
import inspect as Isp, logging, logging.handlers
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.LIB.Functions.Table import ManufactureItemTable as MIT

def main():

    dtNow = DateTime.today()

    strBaseYYYY = str(dtNow.year).zfill(4)
    strBaseMM = str(dtNow.month).zfill(2)
    strBaseDD = str(dtNow.day).zfill(2)
    strBaseHH = str(dtNow.hour).zfill(2)
    strBaseII = str(dtNow.minute).zfill(2)
    strBaseSS = str(dtNow.second).zfill(2)

    strNowDate = strBaseYYYY + "-" + strBaseMM + "-" + strBaseDD
    strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

    print("[DAEMON START : " + strNowDate + " " + strNowTime + "]=====================================")


    try:

        strProcessType = '100001'
        data_1 = '00'
        data_2 = '00'
        data_3 = '00'
        data_4 = '00'
        data_5 = '00'
        data_6 = '00'


        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = strAddLogPath + '/' + strProcessType


        setLogger = ULF.setLogFileV2(logging, LogPath)
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[DAEMON START : " + strNowDate + " " + strNowTime + "]")



        time.sleep(10)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = data_1
        dictSwitchData['data_2'] = data_2
        dictSwitchData['data_3'] = data_3
        dictSwitchData['data_4'] = data_4
        dictSwitchData['data_5'] = data_5
        dictSwitchData['data_6'] = data_6
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        return True

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1

        if data_2 is not None:
            dictSwitchData['data_2'] = data_2

        if data_3 is not None:
            dictSwitchData['data_3'] = data_3

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                     "[=============[SUCCESS END]")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                     "[=============[CRONTAB END]")


if __name__ == '__main__':
    main()
