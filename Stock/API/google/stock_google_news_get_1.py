'%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90'

'https://news.google.com/search?q=%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90%20when%3A1h&hl=ko&gl=KR&ceid=KR%3Ako'

'https://news.google.com/search?q=%22%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90%22%20when%3A1h&hl=ko&gl=KR&ceid=KR%3Ako'

'https://news.google.com/search?q=%22%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90%22%20when%3A1h&hl=ko&gl=KR&ceid=KR%3Ako'



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

        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

        strNowYYYYMMDD = strBaseYYYY + strBaseMM + strBaseDD
        strNowHHMM = strBaseHH + strBaseII

        # 초기값
        strProcessType = '011001'
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

        sqlSelectNaverItemLists = " SELECT * FROM " +ConstTableName.NaverStockItemTable +" WHERE state != '99' "
        sqlSelectNaverItemLists += " AND seq > %s "
        sqlSelectNaverItemLists += " AND sectors_code != '277' "
        sqlSelectNaverItemLists += " ORDER BY seq ASC "
        sqlSelectNaverItemLists += " LIMIT 1 "

        cursorStockFriends.execute(sqlSelectNaverItemLists, (strDBSequence))

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

            procs = []

            intItemLoop = 0
            for rstSelectData in rstSelectDatas:
                strDBSequence = str(int(rstSelectData.get('seq')))
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSequence=> ["+str(intItemLoop)+"][" + str(strDBSequence) + "]")

                strDBSectorsName = rstSelectData.get('item_name')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSectorsName=> ["+str(intItemLoop)+"][" + str(strDBSectorsName) + "]")
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

                strCountryCode = "kr"
                RealtyCallUrl = 'https://news.google.com/search?q='

                RealtyCallUrl += '"'+strDBSectorsName+'" '

                RealtyCallUrl += 'when=A1h&hl=ko&gl=KR&ceid=KR%3Ako'

                RealtyCallUrl = 'https://news.google.com/search?q='
                RealtyCallUrl += '"'+strDBSectorsName+'"'
                RealtyCallUrl += '+when:1d&hl=ko&gl=KR&ceid=KR:ko'

                url = parse.urlparse('https://brownbears.tistory.com?name=불곰&params=123')


                stToday = DateTime.today()

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[stToday]" + str(stToday))

                strDateYYYYMMDD = str(stToday.year).zfill(4) + str(stToday.month).zfill(2) + str(stToday.day).zfill(2)
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateYYYYMMDD]" + str(strDateYYYYMMDD))

                strDateHHIISS = str(stToday.hour).zfill(2) + str(stToday.minute).zfill(2) + str(stToday.second).zfill(2)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateHHIISS]" + str(strDateHHIISS))

                # 파이어폭스 셀리니움 드라이버
                driver = Firefox.defFireBoxDrive()

                strResult = driver.get(RealtyCallUrl)  # 크롤링할 사이트 호출
                htmlSource = driver.page_source  # page_source 얻기
                soup = BeautifulSoup(htmlSource, "html.parser")  # get html

                Tables = soup.select('#yDmH0d > c-wiz > div > main > div.UW0SDc')

                # print("soup => " , soup)

                # ajaxJsonText = Tables[0]

                print("Tables => " , Tables)

                '"국영지앤엠" when:1d'


                'https://news.google.com/search?q=%22%EA%B5%AD%EC%98%81%EC%A7%80%EC%95%A4%EC%97%A0%22%20when%3A1h&hl=ko&gl=KR&ceid=KR%3Ako'




                intItemLoop +=1


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

