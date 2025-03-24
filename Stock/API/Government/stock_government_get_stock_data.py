# 금융위원회_주식시세정보
# 2024-11-25 커밋
#https://www.data.go.kr/iim/api/selectAPIAcountView.do
#https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo

import sys ,os
import time

sys.path.append("D:/PythonProjects/airstock")


import urllib.request, requests
import json
import pymysql
import traceback
import xml
import xml.etree.ElementTree as ET

from Realty.Government.Init import init_conf
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName

from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Lib.CustomException.QuitException import QuitException

def main():

    from Stock.LIB.Functions.Switch import StockSwitchTable
    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
    from Stock.LIB.Functions.Table import ManufactureItemTable as MIT

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

        # 초기값
        strProcessType = '040101'
        strDBSequence = '0'
        strDBSectorsName = '00'
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
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'a', dictSwitchData)

        # DB 연결
        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        sqlSelectItems = "SELECT * FROM " +ConstTableName.NaverStockItemTable +" WHERE state != '99' "
        sqlSelectItems += " AND seq > %s "
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
                strDBSequence = rstSelectData.get('seq')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSequence=> ["+str(intItemLoop)+"][" + str(strDBSequence) + "]")

                strDBSectorsName = rstSelectData.get('item_name')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSectorsName=> ["+str(intItemLoop)+"][" + str(strDBSectorsName) + "]")

                strDBItemCode = rstSelectData.get('item_code')
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(intItemLoop) + "][" + str(strDBItemCode) + "]")

                strDBCountryCode = str(rstSelectData.get('country_code'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBCountryCode=> [" + str(intItemLoop) + "][" + str(strDBCountryCode) + "]")

                strDBUnitPrice = str(rstSelectData.get('unit_price'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBUnitPrice=> [" + str(intItemLoop) + "][" + str(strDBUnitPrice) + "]")

                strDBMultiplication = str(rstSelectData.get('multiplication'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBMultiplication=> [" + str(intItemLoop) + "][" + str(strDBMultiplication) + "]")

                strDBState = str(rstSelectData.get('state'))
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBState=> [" + str(intItemLoop) + "][" + str(strDBState) + "]")


                strItemTableName = MIT.GetItemTableName(strDBItemCode,strDBCountryCode)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemTableName=> [" + str(len(
                    strItemTableName)) + "][" + str(
                    strItemTableName) + "]")

                if len(strItemTableName)< 1:
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                # 테이블 없으면 제작
                bCreateResult = MIT.CheckExistItemTable(strItemTableName, ResStockFriendsConnection)
                if bCreateResult != True:
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


                # 시작월 마지막 월 (12개월 * 30년)
                strCallUrl = 'https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo'
                params = {'serviceKey': init_conf.MolitDecodedAuthorizationKey, 'numOfRows': '20','likeSrtnCd': str(strDBItemCode)}

                while True:

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(intItemLoop) + "][" + str(strDBItemCode) + "]")

                    try:

                        response = requests.get(strCallUrl, params=params)
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "response===> "+str(type(response))+"[" + str(strDBItemCode) + "]")
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " response.status_code => [" + str(
                            intItemLoop) + "][" + str(response.status_code) + "]")

                        if response.status_code == int(500):
                            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                        elif response.status_code == int(200):

                            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " responseContents=> [" + str(
                                intItemLoop) + "][" + str(response.raise_for_status()) + "]")

                            logging.info(
                                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "response===> " + str(type(response)) +
                                str(response))


                            responseContents = response.text  # page_source 얻기

                            # logging.info(
                            #     SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "response===> " + str(type(responseContents)) +
                            #     str(responseContents))


                            ElementResponseRoot = ET.fromstring(responseContents)
                            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "ElementResponseRoot===> " + str(
                            #     type(ElementResponseRoot)) + "[" + str(ElementResponseRoot) + "]")

                            strHeaderResultCode = ElementResponseRoot.find('header').find('resultCode').text
                            strHeaderResultMessage = ElementResponseRoot.find('header').find('resultMsg').text

                            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strHeaderResultCode===> " + str(
                                type(strHeaderResultCode)) + "[" + str(strHeaderResultCode) + "]")
                            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strHeaderResultMessage===> " + str(
                                type(strHeaderResultMessage)) + "[" + str(strHeaderResultMessage) + "]")

                            if strHeaderResultCode == '00':
                                logging.info(
                                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strHeaderResultCode===> " + str(
                                        type(strHeaderResultCode)) + "[" + str(strHeaderResultCode) + "]")
                                break

                        else:
                            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


                    except requests.exceptions.Timeout as e:
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Timeout strCallUrl=> [" + str(intItemLoop) + "][" + str(strCallUrl) + "]")
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Timeout params=> [" + str(intItemLoop) + "][" + str(params) + "]")
                        time.sleep(10)

                    except Exception as e:
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Exception strCallUrl=> [" + str(
                            intItemLoop) + "][" + str(strCallUrl) + "]")
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Exception params=> [" + str(
                            intItemLoop) + "][" + str(params) + "]")
                        time.sleep(10)



                responseContents = response.text  # page_source 얻기
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "responseContents=> [" + str(
                #     intItemLoop) + "][" + str(responseContents) + "]")
                ElementResponseRoot = ET.fromstring(responseContents)
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "ElementResponseRoot=> [" + str(
                #     intItemLoop) + "][" + str(ElementResponseRoot) + "]")

                objectBodyItems = ElementResponseRoot.find('body').find('items')

                for objectBodyItem in objectBodyItems:

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "objectBodyItem=> [" + str(
                        objectBodyItem) + "][" + str(objectBodyItem) + "]")

                    dictInsertItemDatas = dict()
                    dictInsertItemDatas['multiplication'] = strDBMultiplication
                    dictInsertItemDatas['unit_price'] = strDBUnitPrice
                    dictInsertItemDatas['state'] = strDBState
                    # dictInsertItemDatas['daily_seq'] = 0

                    if objectBodyItem.find('basDt') != None:
                        # YYYYMMDD = str(objectBodyItem.find('basDt').text).strip().zfill(8)
                        # HHIISS = '1630'
                        dictInsertItemDatas['YYYYMMDD'] = str(objectBodyItem.find('basDt').text).strip().zfill(8)
                        dictInsertItemDatas['HHII'] = '1630'


                    if objectBodyItem.find('isinCd') != None:
                        strTradeYYYY = str(objectBodyItem.find('isinCd').text).strip().zfill(12)


                    if objectBodyItem.find('itmsNm') != None:
                        strTradeMM = str(objectBodyItem.find('itmsNm').text).strip().zfill(2)
                    if objectBodyItem.find('mrktCtg') != None:
                        strTradeDD = str(objectBodyItem.find('mrktCtg').text).strip().zfill(2)
                    if objectBodyItem.find('clpr') != None:
                        strLastPrice = str(objectBodyItem.find('clpr').text).strip()
                        dictInsertItemDatas['now_price'] = strLastPrice

                    if objectBodyItem.find('vs') != None:
                        strChangePrice = str(objectBodyItem.find('vs').text).strip()
                        dictInsertItemDatas['change_price'] = strChangePrice
                        dictInsertItemDatas['change_flag'] = 'EVEN'
                        if int(strChangePrice) < 0:
                            dictInsertItemDatas['change_flag'] = 'FALL'
                        elif int(strChangePrice) > 0:
                            dictInsertItemDatas['change_flag'] = 'RISE'

                    if objectBodyItem.find('fltRt') != None:
                        strchageRate = str(objectBodyItem.find('fltRt').text).strip()
                        dictInsertItemDatas['change_rate'] = strchageRate

                    if objectBodyItem.find('mkp') != None:
                        strStartPrice = str(objectBodyItem.find('mkp').text).strip()
                        dictInsertItemDatas['start_price'] = strStartPrice
                    if objectBodyItem.find('hipr') != None:
                        strMaxPrice = str(objectBodyItem.find('hipr').text).strip()
                        dictInsertItemDatas['max_price'] = strMaxPrice
                    if objectBodyItem.find('lopr') != None:
                        strMinPrice = str(objectBodyItem.find('lopr').text).strip()
                        dictInsertItemDatas['min_price'] = strMinPrice
                    if objectBodyItem.find('trqu') != None:
                        strTradeVolume = str(objectBodyItem.find('trqu').text).strip()
                        dictInsertItemDatas['trade_volume'] = strTradeVolume
                    if objectBodyItem.find('trPrc') != None:
                        strtradeAmount = str(objectBodyItem.find('trPrc').text).strip()
                        dictInsertItemDatas['trade_amount'] = strtradeAmount
                    if objectBodyItem.find('lstgStCnt') != None:
                        strTotalVolume = str(objectBodyItem.find('lstgStCnt').text).strip()
                        dictInsertItemDatas['total_volume'] = strTotalVolume
                    if objectBodyItem.find('mrktTotAmt') != None:
                        strTotalAmount = str(objectBodyItem.find('mrktTotAmt').text).strip()
                        dictInsertItemDatas['total_amount'] = strTotalAmount

                    #테이블 이름 제작
                    sqlSelectItemDailyData = "SELECT * FROM " + strItemTableName + " "
                    sqlSelectItemDailyData += " WHERE  YYYYMMDD = %s "
                    sqlSelectItemDailyData += " AND  HHII = %s "

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "sqlSelectItemDailyData=> [" + str(
                        sqlSelectItemDailyData) + "][" + str(sqlSelectItemDailyData) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(len(
                        dictInsertItemDatas['YYYYMMDD'])) + "]["+str(type(dictInsertItemDatas['YYYYMMDD']))+"][" + str(
                        dictInsertItemDatas['YYYYMMDD']) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBCountryCode=> [" + str(len(
                        dictInsertItemDatas['HHII'])) + "]["+str(type(dictInsertItemDatas['HHII']))+"][" + str(
                        dictInsertItemDatas['HHII']) + "]")
                    cursorStockFriends.execute(sqlSelectItemDailyData, (dictInsertItemDatas['YYYYMMDD'],dictInsertItemDatas['HHII']))
                    intAffectedCount = cursorStockFriends.rowcount
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intAffectedCount=> [" + str(
                        intAffectedCount) + "]")
                    if intAffectedCount > 0:
                        continue

                    listFieldValues = list()
                    sqlInsertItemTable = " INSERT INTO " + strItemTableName + " SET daily_seq = '0' "

                    for dictInsertItemKey, dictInsertItemValue in dictInsertItemDatas.items():
                        sqlInsertItemTable += ", " + dictInsertItemKey + " = %s"
                        listFieldValues.append(dictInsertItemValue)



                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " sqlInsertItemTable=> [" + str(len(
                        sqlInsertItemTable)) + "]["+str(type(sqlInsertItemTable))+"][" + str(
                        sqlInsertItemTable) + "]")

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " listFieldValues=> [" + str(len(
                        listFieldValues)) + "]["+str(type(listFieldValues))+"][" + str(
                        listFieldValues) + "]")


                    cursorStockFriends.execute(sqlInsertItemTable, listFieldValues)
                    ResStockFriendsConnection.commit()

                    intItemLoop+= 1

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = strNowDate
                dictSwitchData['data_2'] = strDBSequence
                dictSwitchData['data_3'] = strDBSectorsName
                dictSwitchData['data_4'] = intItemLoop
                StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'b', dictSwitchData)
                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "============================time.sleep(1) ")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " ============================time.sleep(1)")
                time.sleep(1)





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
        dictSwitchData['data_1'] = strNowDate
        dictSwitchData['data_2'] = strDBSequence
        dictSwitchData['data_3'] = strDBSectorsName
        dictSwitchData['data_4'] = intItemLoop
        StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'c', dictSwitchData)

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())  + "[ELSE]========================================================")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]========================================================")


if __name__ == '__main__':
    main()