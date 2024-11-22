import sys
sys.path.append("D:/PythonProjects/airstock")
sys.path.append("D:/PythonProjects/airstock/Stock")

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
        strProcessType = '010101'

        LogPath = 'Stock/CronLog_' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        strDBSequence='0'
        strDBSectorsName=''

        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS


        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " [CRONTAB START : " + strNowTime + "]=====================================")



        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = StockSwitchTable.SwitchResultSelectV2(strProcessType)
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
        StockSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)


        strCountryCode = "kr"
        RealtyCallUrl = 'https://finance.naver.com/sise/sise_group.naver?type=upjong'

        stToday = datetime.today()

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[stToday]" + str(stToday))

        strDateYYYYMMDD = str(stToday.year).zfill(4) + str(stToday.month).zfill(2) + str(stToday.day).zfill(2)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateYYYYMMDD]" + str(strDateYYYYMMDD))

        strDateHHIISS = str(stToday.hour).zfill(2) + str(stToday.minute).zfill(2) + str(stToday.second).zfill(2)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateHHIISS]" + str(strDateHHIISS))

        #파이어폭스 셀리니움 드라이버
        driver = Firefox.defFireBoxDrive()

        strResult = driver.get(RealtyCallUrl)  # 크롤링할 사이트 호출
        htmlSource = driver.page_source  # page_source 얻기
        soup = BeautifulSoup(htmlSource, "html.parser")  # get html

        Tables = soup.select('table')
        ajaxJsonText = Tables[0]

        # JsonText = BeautifulSoup(ajaxJsonText , "html.parser")
        #
        # print(JsonText)

        rstMainElements = soup.select_one('#contentarea_left > table > tbody')

        # print("rstMainElements >> ", rstMainElements)

        intLoopTrElements = 0
        rstTrElements = rstMainElements.select('tr')

        sqlUpdateIndustry = "UPDATE " + ConstTableName.NaverIndustryCategoryTable + " SET "
        sqlUpdateIndustry += " state = '01' "
        cursorStockFriends.execute(sqlUpdateIndustry)

        intAffectedCount = cursorStockFriends.rowcount
        if intAffectedCount < 1:
            raise Exception("[intAffectedCount]" + str(intAffectedCount))


        for rstTrElement in rstTrElements:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "intLoopTrElements" +
                  "-------------------------------------------------------------------------------")

            intLoopTrElements += 1
            strRankNumber = str(intLoopTrElements)

            strSelectTDMessages = rstTrElement.select('td')
            if len(strSelectTDMessages) < 3:
                continue

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(strSelectTDMessages))

            nLoopErrorMessage = 0
            strNaverLink = gfg = html.unescape(strSelectTDMessages[0].select_one('a')["href"])
            listURLparsed = urlparse(strNaverLink)


            strNaverLink = gfg = html.unescape(strSelectTDMessages[0].select_one('a')["href"])

            # print(GetLogDef.lineno(__file__), type(parsed),  parsed)
            # print(GetLogDef.lineno(__file__), type(parsed.query), parsed.query)

            DictURLlists = listURLparsed.query.split("&")
            intSectorsCode = ''

            for DictURLlist in DictURLlists:
                # print(GetLogDef.lineno(__file__), type(Dictlist), Dictlist)
                listParseArgs = DictURLlist.split("=")
                # print(GetLogDef.lineno(__file__), type(listParseArgs), listParseArgs)
                if listParseArgs[0] == 'no':
                    strSectorsCode = str(listParseArgs[1])

                if listParseArgs[0] == 'type':
                    strSectorsType = str(listParseArgs[1])

            strNaverIndustryName = strSelectTDMessages[0].getText().strip()
            strNaverIndustryPercent = strSelectTDMessages[1].getText().strip()
            strNaverIndustryPercent = str(strNaverIndustryPercent.replace('%',''))
            floatNaverIndustryPercent = float(strNaverIndustryPercent)

            strNaverIndustryTotalCount = strSelectTDMessages[2].getText().strip()

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strIndustryPercent => "+  str(strNaverIndustryPercent))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "floatIndustryPercent => "+ str(floatNaverIndustryPercent))

            strChange = 'EVEN'
            if floatNaverIndustryPercent < 0:
                strChange = 'FALL'
            elif floatNaverIndustryPercent > 0:
                strChange = 'RISE'

            sqlSelectIndustry = "SELECT * FROM " + ConstTableName.NaverIndustryCategoryTable + " "
            sqlSelectIndustry += " WHERE sectors_code = %s "
            sqlSelectIndustry += " ORDER BY seq DESC LIMIT 1 "
            cursorStockFriends.execute(sqlSelectIndustry, (strSectorsCode))
            rstSelectDatas = cursorStockFriends.fetchone()

            boolUpdateFlag = False
            if rstSelectDatas != None:
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + "[rstSelectDatas]" + str(rstSelectDatas))

                strDBSequence = str(rstSelectDatas['seq'])
                strDBSectorsName = rstSelectDatas['sectors_name']
                intDBTotalCount = rstSelectDatas['total_count']
                strDBTotalCount = str(rstSelectDatas['total_count'])

                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + "[strDBSectorsName]" + str(strDBSectorsName))
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + "[intDBTotalCount]" + str(intDBTotalCount))

                if strNaverIndustryName != strDBSectorsName:
                    boolUpdateFlag = True

                if strNaverIndustryTotalCount != intDBTotalCount:
                    boolUpdateFlag = True


                if boolUpdateFlag == True:
                    sqlUpdateIndustry = " UPDATE  " + ConstTableName.NaverIndustryCategoryTable + " SET "
                    sqlUpdateIndustry += " sectors_name = %s "
                    sqlUpdateIndustry += " ,total_count = %s "
                    sqlUpdateIndustry += " ,state = '00' "
                    sqlUpdateIndustry += " ,modify_date = NOW() "
                    sqlUpdateIndustry += " WHERE sectors_code = %s"
                    cursorStockFriends.execute(sqlUpdateIndustry, (strNaverIndustryName ,strDBTotalCount, strSectorsCode))
                    intAffectedCount = cursorStockFriends.rowcount
                    if intAffectedCount < 1:
                        raise Exception("[intAffectedCount]" + str(intAffectedCount))

            else:
                sqlInsertIndustry = "INSERT INTO " + ConstTableName.NaverIndustryCategoryTable + " SET "
                sqlInsertIndustry += " YYYYMMDD = %s "
                sqlInsertIndustry += ",HHIISS = %s "
                sqlInsertIndustry += ",sectors_code = %s "
                sqlInsertIndustry += ",sectors_name = %s "
                sqlInsertIndustry += ",country_code = %s "
                sqlInsertIndustry += ",change_flag = %s "
                sqlInsertIndustry += ",total_count = %s "
                sqlInsertIndustry += ",change_rate = %s "
                sqlInsertIndustry += ",ranking = %s "
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + sqlInsertIndustry +"["+str(strDateYYYYMMDD)+"]"+"["+  str(strDateHHIISS)+"]"+"["+str(strSectorsCode)+"]"+"["+str(strNaverIndustryName)+"]"+"["+str(strCountryCode)+"]"+"["+str(strChange)+"]"+"["+str(strNaverIndustryTotalCount)+"]"+"["+str(strNaverIndustryPercent)+"]"+"["+str(strRankNumber)+"]")
                cursorStockFriends.execute(sqlInsertIndustry, (strDateYYYYMMDD,strDateHHIISS,strSectorsCode,strNaverIndustryName,strCountryCode,strChange,strNaverIndustryTotalCount,strNaverIndustryPercent,strRankNumber))

            sqlInsertIndustryLog = "INSERT INTO " + ConstTableName.NaverIndustryCategoryLogTable + " SET "
            sqlInsertIndustryLog += " YYYYMMDD = %s "
            sqlInsertIndustryLog += ",HHIISS = %s "
            sqlInsertIndustryLog += ",sectors_code = %s "
            sqlInsertIndustryLog += ",sectors_name = %s "
            sqlInsertIndustryLog += ",country_code = %s "
            sqlInsertIndustryLog += ",change_flag = %s "
            sqlInsertIndustryLog += ",total_count = %s "
            sqlInsertIndustryLog += ",change_rate = %s "
            sqlInsertIndustryLog += ",ranking = %s "
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + sqlInsertIndustryLog + "[" + str(
                strDateYYYYMMDD) + "]" + "[" + str(strDateHHIISS) + "]" + "[" + str(strSectorsCode) + "]" + "[" + str(
                strNaverIndustryName) + "]" + "[" + str(strCountryCode) + "]" + "[" + str(strChange) + "]" + "[" + str(
                strNaverIndustryTotalCount) + "]" + "[" + str(strNaverIndustryPercent) + "]" + "[" + str(
                strRankNumber) + "]")
            cursorStockFriends.execute(sqlInsertIndustryLog, (strDateYYYYMMDD, strDateHHIISS, strSectorsCode, strNaverIndustryName, strCountryCode, strChange, strNaverIndustryTotalCount, strNaverIndustryPercent, strRankNumber))
            ResStockFriendsConnection.commit()

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = strNowDate
            dictSwitchData['data_2'] = strDBSequence
            dictSwitchData['data_3'] = strDBSectorsName
            StockSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        StockSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


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
        StockSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())  + "[ELSE]========================================================")

    finally:
        driver.quit()  # 크롬 브라우저 닫기
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]========================================================")


# if __name__ == '__main__':
#     main()