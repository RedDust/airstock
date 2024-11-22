import sys
sys.path.append("D:/PythonProjects/airstock")
sys.path.append("D:/PythonProjects/airstock/Stock")

import urllib.request
import requests
import json
import time
import re
import pandas as pd
import html
import pymysql
import traceback
import inspect as Isp, logging, logging.handlers

from urllib.parse import urlparse
from datetime import datetime , timedelta as TimeDelta
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

        strProcessType = '010102'

        LogPath = 'Stock/CronLog_' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()

        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS

        LogPath = 'Stock/CronLog_' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        strSequence='0'

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
            strSequence = rstResult.get('data_2')
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " [strSequence : " + str(strSequence) + "]")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = strNowDate
        dictSwitchData['data_2'] = strSequence
        StockSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)


        sqlSelectIndustryCategory = "SELECT * FROM " +ConstTableName.NaverIndustryCategoryTable +" WHERE state='00' "
        sqlSelectIndustryCategory += " AND seq > '"+strSequence+"' "
        sqlSelectIndustryCategory += " ORDER BY seq ASC "
        # sqlSelectIndustryCategory += " limit 1 "
        cursorStockFriends.execute(sqlSelectIndustryCategory)
        rstSelectDatas = cursorStockFriends.fetchall()
        if rstSelectDatas != None:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstSelectDatas >> " + str(rstSelectDatas))

        for rstSelectData in rstSelectDatas:
            strSequence = str(rstSelectData.get('seq'))
            strSectorCode = str(rstSelectData.get('sectors_code'))
            strSectorName = str(rstSelectData.get('sectors_name'))
            strCountryCode = str(rstSelectData.get('country_code'))

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strSectorCode >> " + str(strSectorCode))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strCountryCode >> " + str(strCountryCode))

            RealtyCallUrl = 'https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no='+strSectorCode
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[RealtyCallUrl]" + str(RealtyCallUrl))

            strDateYYYYMMDD = str(dtNow.year).zfill(4) + str(dtNow.month).zfill(2) + str(dtNow.day).zfill(2)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateYYYYMMDD]" + str(strDateYYYYMMDD))

            strDateHHIISS = str(dtNow.hour).zfill(2) + str(dtNow.minute).zfill(2) + str(dtNow.second).zfill(2)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDateHHIISS]" + str(strDateHHIISS))

            #파이어폭스 셀리니움 드라이버
            driver = Firefox.defFireBoxDrive()

            strResult = driver.get(RealtyCallUrl)  # 크롤링할 사이트 호출
            htmlSource = driver.page_source  # page_source 얻기

            soup = BeautifulSoup(htmlSource, "html.parser")  # get html

            rstMainElement = soup.select_one('#contentarea > div:nth-child(5) > table')

            # print("rstMainElement> " , rstMainElement )
            # contentarea > div:nth-child(5) > table > tbody
            rstMainTBobyElement = rstMainElement.select_one("tbody")

            rstMainTrElements =rstMainTBobyElement.select("tr")

            intTrLoop = 0

            for rstMainTrElement in rstMainTrElements:

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "rstMainTrElement>["+str(intTrLoop)+"] ============================================" )
                rstMainTrTdElements = rstMainTrElement.select("td")

                if len(rstMainTrTdElements) < 5:
                    continue

                intTdLoop = 0
                for rstMainTrTdElement in rstMainTrTdElements:
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "rstMainTrTdElement>["+str(intTdLoop)+"]" + str(rstMainTrTdElement))
                    logging.info(rstMainTrTdElement.getText().strip())
                    intTdLoop+=1

                strNaverLink = html.unescape(rstMainTrTdElements[0].select_one('a')["href"])

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strNaverLink>" + str(strNaverLink))
                listURLparsed = urlparse(strNaverLink)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstMainTrTdElement> rstMainTrTdElement>")
                DictURLlists = listURLparsed.query.split("&")
                strItemName = rstMainTrTdElements[0].select_one('a').getText()

                strItemCode = ''
                for DictURLlist in DictURLlists:
                    # print(GetLogDef.lineno(__file__), type(Dictlist), Dictlist)
                    listParseArgs = DictURLlist.split("=")
                    # print(GetLogDef.lineno(__file__), type(listParseArgs), listParseArgs)
                    if listParseArgs[0] == 'code':
                        strItemCode = str(listParseArgs[1])

                strItemNameWithMarket = rstMainTrTdElements[0].getText().strip()
                strTodayPrice = rstMainTrTdElements[1].getText().strip().replace(',','')

                strUpDownPriceRoot = rstMainTrTdElements[2].getText().strip().replace(',','')
                strUpDownPrice = re.sub(r'[^0-9]', '', strUpDownPriceRoot)
                strUpDown = re.sub(r'[^ㄱ-ㅎ가-힣]', '', strUpDownPriceRoot)

                strUpDownRate = rstMainTrTdElements[3].getText().strip().replace('%','')
                strLastBuyPrice = rstMainTrTdElements[4].getText().strip().replace(',','')
                strLastSellPrice = rstMainTrTdElements[5].getText().strip().replace(',','')
                strTradingVolume = rstMainTrTdElements[6].getText().strip().replace(',','')
                strTradingAmount = rstMainTrTdElements[7].getText().strip().replace(',','')
                strBeforeTradingVolume = rstMainTrTdElements[8].getText().strip().replace(',','')

                strMarketCode = 'ETC'
                if strItemNameWithMarket.endswith("*") == True:
                    strMarketCode = 'KOSDAQ'

                strUpDownCode = ''
                if strUpDown == '상승':
                    strUpDownCode='RISE'
                elif strUpDown == '보합':
                    strUpDownCode = 'FALL'
                elif strUpDown == '하락':
                    strUpDownCode = 'EVEN'

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemCode=> [" + str(len(strItemCode)) + "] " + str(strItemCode))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemName=> [" + str(len(strItemName)) + "] " + str(strItemName))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemNameWithMarket=> [" + str(len(strItemNameWithMarket)) + "] " + str(strItemNameWithMarket))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strMarketCode=> [" + str(len(strMarketCode)) + "] " + str(strMarketCode))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strSectorCode=> [" + str(len(strSectorCode)) + "] " + str(strSectorCode))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strSectorName=> [" + str(len(strSectorName)) + "] " + str(strSectorName))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strTradingAmount=> [" + str(len(strTradingAmount)) + "] " + str(strTradingAmount))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strMarketCode=> [" + str(len(strUpDownCode)) + "] " + str(strUpDownCode))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strTradingVolume=> [" + str(len(strTradingVolume)) + "] " + str(strTradingVolume))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strLastSellPrice=> [" + str(len(strLastSellPrice)) + "] " + str(strLastSellPrice))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strLastBuyPrice=> [" + str(len(strLastBuyPrice)) + "] " + str(strLastBuyPrice))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strUpDownRatio=> [" + str(len(strUpDownRate)) + "] " + str(strUpDownRate))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strUpDown=> [" + str(len(strUpDown)) + "] " + str(strUpDown))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strUpDownPrice=> [" + str(len(strUpDownPrice)) + "] " + str(strUpDownPrice))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strTodayPrice=> [" + str(len(strTodayPrice)) + "] " + str(strTodayPrice))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strBeforeTradingVolume=> ["+str(len(strBeforeTradingVolume)) + "] " + str(strBeforeTradingVolume))

                sqlSelectStockItem = " SELECT * FROM " + ConstTableName.NaverStockItemTable
                sqlSelectStockItem += " WHERE item_code ='"+strItemCode+"' "
                sqlSelectStockItem += " AND country_code ='" + strCountryCode + "' "
                cursorStockFriends.execute(sqlSelectStockItem)
                intAffectedCount = cursorStockFriends.rowcount
                if intAffectedCount < 1:
                    #insert
                    sqlInsertStockItem = " INSERT INTO " + ConstTableName.NaverStockItemTable + " SET "
                    sqlInsertStockItem += " item_code = %s "
                    sqlInsertStockItem += ", item_name = %s "
                    sqlInsertStockItem += ", country_code = %s "
                    sqlInsertStockItem += ", market_code = %s "
                    sqlInsertStockItem += ", sectors_code = %s "
                    sqlInsertStockItem += ", sectors_name = %s "
                    sqlInsertStockItem += ", now_price = %s "
                    sqlInsertStockItem += ", updown_code = %s "
                    sqlInsertStockItem += ", updown_price = %s "
                    sqlInsertStockItem += ", updown_rate = %s "
                    sqlInsertStockItem += ", trade_volume = %s "
                    sqlInsertStockItem += ", trade_amount = %s "
                    # sqlInsertStockItem += ", market_cap = %s "
                    # sqlInsertStockItem += ", market_volume = %s "
                    # sqlInsertStockItem += ", unit_price = %s "
                    # sqlInsertStockItem += ", PER = %s "
                    # sqlInsertStockItem += ", EPS = %s "
                    # sqlInsertStockItem += ", PBR = %s "
                    # sqlInsertStockItem += ", div_yield = %s "
                    # sqlInsertStockItem += ", foreign_rate = %s "
                    # sqlInsertStockItem += ", ranking = %s "
                    # sqlInsertStockItem += ", multiplication = %s "
                    # sqlInsertStockItem += ", focus = %s "
                    # sqlInsertStockItem += ", position = %s "
                    # sqlInsertStockItem += ", processing_state = %s "

                    listInsertArguments = [strItemCode, strItemName, strCountryCode, strMarketCode, strSectorCode,
                                            strSectorName, strTradingAmount, strUpDownCode,strUpDownPrice, strUpDownRate,
                                            strTradingVolume,strTradingAmount]
                    cursorStockFriends.execute(sqlInsertStockItem , listInsertArguments)

                    sqlInsertStockItem = " INSERT INTO " + ConstTableName.NaverStockItemLogTable + " SET "
                    sqlInsertStockItem += " YYYYMMDD = %s "
                    sqlInsertStockItem += ", item_code = %s "
                    sqlInsertStockItem += ", item_name = %s "
                    sqlInsertStockItem += ", country_code = %s "
                    sqlInsertStockItem += ", market_code = %s "
                    sqlInsertStockItem += ", sectors_code = %s "
                    sqlInsertStockItem += ", sectors_name = %s "
                    sqlInsertStockItem += ", now_price = %s "
                    sqlInsertStockItem += ", updown_code = %s "
                    sqlInsertStockItem += ", updown_price = %s "
                    sqlInsertStockItem += ", updown_rate = %s "
                    sqlInsertStockItem += ", trade_volume = %s "
                    sqlInsertStockItem += ", trade_amount = %s "
                    listInsertArguments = [strNowDate, strItemCode, strItemName, strCountryCode, strMarketCode, strSectorCode,
                                            strSectorName, strTradingAmount, strUpDownCode,strUpDownPrice, strUpDownRate,
                                            strTradingVolume,strTradingAmount]
                    cursorStockFriends.execute(sqlInsertStockItem , listInsertArguments)

                elif intAffectedCount == 1:

                    #update
                    rstSelectItemData = cursorStockFriends.fetchone()
                    #
                    strDBItemSeq = str(rstSelectItemData.get('seq'))
                    # strDBItemItemCode = rstSelectItemData.get('item_code')
                    # strDBItemItemName = rstSelectItemData.get('item_name')
                    # strDBItemCountryCode = rstSelectItemData.get('country_code')
                    # strDBItemMarketCode = rstSelectItemData.get('market_code')
                    # strDBItemSectorsCode = rstSelectItemData.get('sectors_code')
                    # strDBItemSectorsName = rstSelectItemData.get('sectors_name')
                    # strDBItemNowPrice = rstSelectItemData.get('now_price')
                    # strDBItemUpdownCode= rstSelectItemData.get('updown_code')
                    # strDBItemUpdownPrice= rstSelectItemData.get('updown_price')
                    # strDBItemUpdownRate = rstSelectItemData.get('updown_rate')
                    # strDBItemTradeVolume = rstSelectItemData.get('trade_volume')
                    # strDBItemTradePrice = rstSelectItemData.get('trade_price')


                    sqlUpdateStockItem = " UPDATE " + ConstTableName.NaverStockItemTable + " SET "
                    sqlUpdateStockItem += " item_code = %s "
                    sqlUpdateStockItem += ", item_name = %s "
                    sqlUpdateStockItem += ", country_code = %s "
                    sqlUpdateStockItem += ", market_code = %s "
                    sqlUpdateStockItem += ", sectors_code = %s "
                    sqlUpdateStockItem += ", sectors_name = %s "
                    sqlUpdateStockItem += ", now_price = %s "
                    sqlUpdateStockItem += ", updown_code = %s "
                    sqlUpdateStockItem += ", updown_price = %s "
                    sqlUpdateStockItem += ", updown_rate = %s "
                    sqlUpdateStockItem += ", trade_volume = %s "
                    sqlUpdateStockItem += ", trade_amount = %s "
                    sqlUpdateStockItem += ", modify_date = NOW() "
                    sqlUpdateStockItem += " WHERE seq = %s "

                    listUpdateArguments = [strItemCode, strItemName, strCountryCode, strMarketCode, strSectorCode,
                                            strSectorName, strTradingAmount, strUpDownCode, strUpDownPrice, strUpDownRate,
                                            strTradingVolume, strTradingAmount, strDBItemSeq]
                    cursorStockFriends.execute(sqlUpdateStockItem , listUpdateArguments)


                    sqlSelectStockItemLog = " SELECT * FROM " + ConstTableName.NaverStockItemLogTable + " "
                    sqlSelectStockItemLog += " WHERE item_code = %s  AND country_code = %s AND YYYYMMDD = %s "
                    listSelectArguments = [strItemCode, strCountryCode, strNowDate]
                    cursorStockFriends.execute(sqlSelectStockItemLog, listSelectArguments)

                    intAffectedCount = cursorStockFriends.rowcount
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                            intAffectedCount) + "]")
                    if intAffectedCount < 1:
                        logging.info( SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                                intAffectedCount) + "]")

                        sqlInsertStockItem = " INSERT INTO " + ConstTableName.NaverStockItemLogTable + " SET "
                        sqlInsertStockItem += " YYYYMMDD = %s "
                        sqlInsertStockItem += ", item_code = %s "
                        sqlInsertStockItem += ", item_name = %s "
                        sqlInsertStockItem += ", country_code = %s "
                        sqlInsertStockItem += ", market_code = %s "
                        sqlInsertStockItem += ", sectors_code = %s "
                        sqlInsertStockItem += ", sectors_name = %s "
                        sqlInsertStockItem += ", now_price = %s "
                        sqlInsertStockItem += ", updown_code = %s "
                        sqlInsertStockItem += ", updown_price = %s "
                        sqlInsertStockItem += ", updown_rate = %s "
                        sqlInsertStockItem += ", trade_volume = %s "
                        sqlInsertStockItem += ", trade_amount = %s "
                        listInsertArguments = [strNowDate, strItemCode, strItemName, strCountryCode, strMarketCode, strSectorCode,
                                                strSectorName, strTradingAmount, strUpDownCode,strUpDownPrice, strUpDownRate,
                                                strTradingVolume,strTradingAmount]
                        for listUpdateArgument in listUpdateArguments:
                            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +"listUpdateArgument>>"+str(listUpdateArgument))

                        cursorStockFriends.execute(sqlInsertStockItem , listInsertArguments)

                    else:
                        rstSelectItemLogData = cursorStockFriends.fetchone()
                        strDBLogSeq = rstSelectItemLogData.get('seq')

                        sqlUpdateStockItemLog = " UPDATE " + ConstTableName.NaverStockItemTable + " SET "
                        sqlUpdateStockItemLog += "item_name = %s "
                        sqlUpdateStockItemLog += ", market_code = %s "
                        sqlUpdateStockItemLog += ", sectors_code = %s "

                        sqlUpdateStockItemLog += ", sectors_name = %s "
                        sqlUpdateStockItemLog += ", now_price = %s "
                        sqlUpdateStockItemLog += ", updown_code = %s "
                        sqlUpdateStockItemLog += ", updown_price = %s "
                        sqlUpdateStockItemLog += ", updown_rate = %s "

                        sqlUpdateStockItemLog += ", trade_volume = %s "
                        sqlUpdateStockItemLog += ", trade_amount = %s "
                        sqlUpdateStockItemLog += " WHERE seq = %s "

                        listUpdateArguments = [ strItemName, strMarketCode, strSectorCode,
                                                strSectorName, strTradingAmount, strUpDownCode,strUpDownPrice, strUpDownRate,
                                                strTradingVolume,strTradingAmount,strDBLogSeq]

                        for listUpdateArgument in listUpdateArguments:
                            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +"listUpdateArgument>>"+str(listUpdateArgument))


                        cursorStockFriends.execute(sqlUpdateStockItemLog, listUpdateArguments)

                else:
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount >> " + str(intAffectedCount))

                    raise Exception(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'sqlSelectStockItem => [' + str(sqlSelectStockItem) + ']')

                ResStockFriendsConnection.commit()

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = strNowDate
                dictSwitchData['data_2'] = strSequence
                StockSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            driver.quit()  # 크롬 브라우저 닫기
            time.sleep(2)
            intTrLoop+=1

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
        dictSwitchData['data_2'] = strSequence
        StockSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())  + "[SUCCESS]========================================================")

    finally:
        driver.quit()  # 크롬 브라우저 닫기
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())  + "[Finally END]")
