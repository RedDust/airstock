#네이버 트랜드 검색

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
from threading import Thread
from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName
from Stock.LIB.SeleniumModule.Windows import Chrome, Firefox
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

    try:

        dtNow = DateTime.today()

        strBaseYYYY = str(dtNow.year).zfill(4)
        strBaseMM = str(dtNow.month).zfill(2)
        strBaseDD = str(dtNow.day).zfill(2)
        strBaseHH = str(dtNow.hour).zfill(2)
        strBaseII = str(dtNow.minute).zfill(2)
        strBaseSS = str(dtNow.second).zfill(2)

        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

        strNowYYYYMMDD = strBaseYYYY + strBaseMM + strBaseDD
        strNowHHMM = strBaseHH + strBaseII

        # 초기값
        strProcessType = '011011'
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

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
        # cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)

        intAffectedCount = 1

        # 시작월 마지막 월 (12개월 * 30년)

        client_id = "BSDRfoN140uPmYW2WLax"
        client_secret = "CrycbI5dEn"

        url = "https://openapi.naver.com/v1/datalab/search"
        body = "{\"startDate\":\"2024-01-01\",\"endDate\":\"2024-12-31\",\"timeUnit\":\"month\",\"keywordGroups\":[{\"groupName\":\"한글\",\"keywords\":[\"한글\",\"korean\"]},{\"groupName\":\"영어\",\"keywords\":[\"영어\",\"english\"]}],\"device\":\"pc\",\"ages\":[\"1\",\"2\"],\"gender\":\"f\"}";

        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id", client_id)
        request.add_header("X-Naver-Client-Secret", client_secret)

        request.add_header("Content-Type", "application/json")
        response = urllib.request.urlopen(request, data=body.encode("utf-8"))
        rescode = response.getcode()
        if (rescode == 200):
            response_body = response.read()
            print(response_body.decode('utf-8'))

        else:
            print("Error Code:" + rescode)

        strResponseDec = response_body.decode('utf-8')

        print("strResponseDec==> " , type(strResponseDec), strResponseDec)

        dictResponseDec = json.loads(strResponseDec)

        print("dictResponseDec==> ", type(dictResponseDec), dictResponseDec)

        # listResponse = hex(response_body)
        #
        # print("listResponse==> ", type(listResponse), listResponse)


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = strNowDate
        dictSwitchData['data_2'] = strDBSequence
        dictSwitchData['data_3'] = strDBSectorsName
        dictSwitchData['data_4'] = intItemLoop
        dictSwitchData['data_5'] = intAffectedCount
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

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

