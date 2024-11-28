# 거래중지 및 거래 유의 종목 업데이트
# 2024-11-25 커밋


import sys
sys.path.append("D:/PythonProjects/airstock")
sys.path.append("D:/PythonProjects/airstock/Stock")

import os
import urllib.request
import requests
import json
import time
import re
import pandas as pd
import inspect as Isp, logging, logging.handlers

import html
import pymysql
import traceback

from urllib.parse import urlparse
from datetime import datetime , timedelta as TimeDelta
from Stock.CONFIG import ConstTableName
from Init.Functions.Logs import GetLogDef as SLog

from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.CONFIG import ConstTableName

from Stock.LIB.SeleniumModule.Windows import Chrome, Firefox
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Stock.LIB.Functions.Switch import StockSwitchTable

def main():
    try:
        dtNow = datetime.today()

        strBaseYYYY = str(dtNow.year).zfill(4)
        strBaseMM = str(dtNow.month).zfill(2)
        strBaseDD = str(dtNow.day).zfill(2)
        strBaseHH = str(dtNow.hour).zfill(2)
        strBaseII = str(dtNow.minute).zfill(2)
        strBaseSS = str(dtNow.second).zfill(2)
        strProcessType = '010203'

        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = 'Stock/'+strAddLogPath+'/'+ strProcessType

        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        strDBSequence='0'
        strDBSectorsName=''
        intBeforeCount = 0
        intProcessCount = 0
        strStockItemCode = ''
        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " [CRONTAB START : " + strNowTime + "]=====================================")


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = StockSwitchTable.SwitchResultSelectV2(logging, strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strResult : " + str(strResult) + "]")
            quit(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) +  'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strResult : " + str(strResult) + "]")
            quit(SLog.Ins(Isp.getframeinfo,
                          Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '20':
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strResult : " + str(strResult) + "]")
            strDBSequence = rstResult.get('data_2')
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strDBSequence : " + str(strDBSequence) + "]")



        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = strNowDate
        dictSwitchData['data_2'] = strDBSequence
        dictSwitchData['data_3'] = strDBSectorsName
        StockSwitchTable.SwitchResultUpdateV2(logging,strProcessType, 'a', dictSwitchData)

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)



        strCountryCode = "kr"

        dictRealtyCallTypes = dict()
        dictRealtyCallTypes['caution'] = '04'
        dictRealtyCallTypes['warning'] = '05'
        dictRealtyCallTypes['risk'] = '06'


        # 파이어폭스 셀리니움 드라이버
        driver = Firefox.defFireBoxDrive()

        for strCallTypeKey, strCallTypeValue in dictRealtyCallTypes.items():

            sqlUpdateStockItem = " UPDATE " + ConstTableName.NaverStockItemTable + " SET "
            sqlUpdateStockItem += " state = '00' "
            sqlUpdateStockItem += " WHERE state = %s "
            cursorStockFriends.execute(sqlUpdateStockItem , strCallTypeValue)
            intBeforeCount = cursorStockFriends.rowcount


            time.sleep(2)

            RealtyCallUrl = 'https://finance.naver.com/sise/investment_alert.naver?type=' + str(strCallTypeKey)
            stToday = datetime.today()

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[stToday]" + str(stToday))

            strDateYYYYMMDD = str(stToday.year).zfill(4) + str(stToday.month).zfill(2) + str(stToday.day).zfill(2)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateYYYYMMDD]" + str(strDateYYYYMMDD))

            strDateHHIISS = str(stToday.hour).zfill(2) + str(stToday.minute).zfill(2) + str(stToday.second).zfill(2)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateHHIISS]" + str(strDateHHIISS))
            #

            # contentarea > div.box_type_l


            strResult = driver.get(RealtyCallUrl)  # 크롤링할 사이트 호출
            htmlSource = driver.page_source  # page_source 얻기
            soup = BeautifulSoup(htmlSource, "html.parser")  # get html

            # print(soup)

            rstTableTbodyElements = soup.select_one('#contentarea > div.box_type_l > table:nth-child(3) > tbody')
            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstTableTbodyElements]" + str(len(
            #     rstTableTbodyElements)) + str(
            #     rstTableTbodyElements))

            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstTableTbodyElements]" + str(
            #     rstTableTbodyElements))


            rstTableTbodyTrElements = rstTableTbodyElements.select('tr')

            intProcessCount = 0
            for rstTableTbodyTrElement in rstTableTbodyTrElements:
                intProcessCount += 1
                strStockItemCode = ''
                rstTableTbodyTrTdElements = rstTableTbodyTrElement.select('td')
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstTableTbodyTrTdElements]" + str(len(rstTableTbodyTrTdElements)) + str(
                #     rstTableTbodyTrTdElements))
                if len(rstTableTbodyTrTdElements) == 9:

                    # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstTableTbodyTrTdElements]" + str(rstTableTbodyTrTdElements))

                    strHaltTag = rstTableTbodyTrTdElements[1].select_one('a')

                    strNaverLink = html.unescape(strHaltTag["href"])
                    listURLparsed = urlparse(strNaverLink)
                    DictURLlists = listURLparsed.query.split("&")
                    for DictURLlist in DictURLlists:
                        # print(GetLogDef.lineno(__file__), type(Dictlist), Dictlist)
                        listParseArgs = DictURLlist.split("=")
                        # print(GetLogDef.lineno(__file__), type(listParseArgs), listParseArgs)
                        if listParseArgs[0] == 'code':
                            strStockItemCode = str(listParseArgs[1])


                    print("strStockItemCode =>", strStockItemCode)

                    sqlSelectItems = "SELECT * FROM " + ConstTableName.NaverStockItemTable + " WHERE "
                    sqlSelectItems += " item_code = %s and state = '00' LIMIT 1 FOR UPDATE "
                    cursorStockFriends.execute(sqlSelectItems, (strStockItemCode))
                    intAffectedCount = cursorStockFriends.rowcount
                    if intAffectedCount > 0:
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                            intAffectedCount) + "]")

                        rstSelectItemData = cursorStockFriends.fetchone()
                        strDBSequence = rstSelectItemData.get('seq')

                        sqlUpdateStockItem = " UPDATE " + ConstTableName.NaverStockItemTable + " SET "
                        sqlUpdateStockItem += " state = %s "
                        sqlUpdateStockItem += ", modify_date = NOW() "
                        sqlUpdateStockItem += " WHERE seq = %s "

                        listUpdateArguments = [strDBSequence]
                        for listUpdateArgument in listUpdateArguments:
                            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "listUpdateArgument>>" + str(
                                listUpdateArgument))
                        cursorStockFriends.execute(sqlUpdateStockItem, (strCallTypeValue , listUpdateArguments))
                        ResStockFriendsConnection.commit()
                    else:
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                            intAffectedCount) + "]")

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = str(strNowDate)
                dictSwitchData['data_2'] = str(strDBSequence)
                dictSwitchData['data_3'] = strStockItemCode
                StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'b', dictSwitchData)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
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
        dictSwitchData['data_1'] = str(strNowDate)
        dictSwitchData['data_2'] = str(strDBSequence)
        dictSwitchData['data_3'] = strStockItemCode
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())  + "[ELSE]========================================================")

    finally:
        driver.quit()  # 크롬 브라우저 닫기
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]========================================================")


if __name__ == '__main__':
    main()