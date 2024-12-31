# 금융위원회_주식시세정보
# 2024-11-25 커밋
#https://www.data.go.kr/iim/api/selectAPIAcountView.do
#https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo

import sys ,os
import time

sys.path.append("D:/PythonProjects/airstock")

import urllib.request, requests
import json, re
import pymysql
import traceback
import xml, html
import xml.etree.ElementTree as ET

from Realty.Government.Init import init_conf
from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName

from Stock.CONFIG import ConstTableName
from Stock.LIB.SeleniumModule.Windows import Chrome, Firefox
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup
from Lib.CustomException.QuitException import QuitException

def main():

    from Stock.LIB.Functions.Switch import StockSwitchTable
    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF

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
        strProcessType = '010002'
        strDBSequence = '0'
        strDBSectorsName = '00'
        intItemLoop = 0
        listUpdateColumn=''
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

        # if strResult == '20':
        #     raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

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


        # 파이어폭스 셀리니움 드라이버
        driver = Firefox.defFireBoxDrive()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstSelectDatas >> " + str())



        listIndexs = []

        listIndexs.append('FX_USDKRW')
        listIndexs.append('FX_EURKRW')
        listIndexs.append('FX_JPYKRW')
        listIndexs.append('FX_CNYKRW')

        strFromCountryCode = 'us'
        strToCountryCode = 'kr'
        strYYYYMMDD='00000000'
        intInsertedID=0

        for listIndex in listIndexs:

            if listIndex == 'FX_USDKRW':
                strFromCountryCode = 'us'
            elif listIndex == 'FX_EURKRW':
                strFromCountryCode = 'eu'
            elif listIndex == 'FX_JPYKRW':
                strFromCountryCode = 'jp'
            elif listIndex == 'FX_CNYKRW':
                strFromCountryCode = 'cn'


            intNowPage = 361
            bStopFlag = False

            while True:

                RealtyCallUrl = 'https://finance.naver.com/marketindex/exchangeDailyQuote.naver?marketindexCd=' + listIndex
                RealtyCallUrl += '&page=' + str(intNowPage)

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[RealtyCallUrl]" + str(RealtyCallUrl))

                strResult = driver.get(RealtyCallUrl)  # 크롤링할 사이트 호출
                htmlSource = driver.page_source  # page_source 얻기

                soup = BeautifulSoup(htmlSource, "html.parser")  # get html
                # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[soup][" + str(soup)+"]")
                #
                # # [2. 코스피 / 코스닥 / 코넥스 조회 ]###################################################################################
                rstTbodyElement = soup.select_one('body > div > table > tbody')
                # rstNnaviTbodyElement = soup.select_one('body > div > table.Nnavi > tbody > tr > td.pgRR')
                #
                rstTbodyTrElements = rstTbodyElement.select('tr')
                #
                # print("rstTbodyTrElements => " , len(rstTbodyTrElements) , rstTbodyTrElements)

                if len(rstTbodyTrElements) < 1:
                    bStopFlag = True

                for rstTbodyTrElement in rstTbodyTrElements:
                    # print("rstTbodyTrElement => ", rstTbodyTrElement)
                    rstTbodyTrTdElements = rstTbodyTrElement.select('td')
                #
                    print("==================================================================" , len(rstTbodyTrTdElements))
                    intTdLoopCnt = 0
                    if len(rstTbodyTrTdElements) != 7:
                        continue
                    print("rstTbodyTrTdElements => ", 0, rstTbodyTrTdElements[0].getText().strip())
                    print("rstTbodyTrTdElements => ", 1, rstTbodyTrTdElements[1].getText().strip())
                    print("rstTbodyTrTdElements => ", 2, rstTbodyTrTdElements[2].getText().strip())
                    print("rstTbodyTrTdElements => ", 'img', rstTbodyTrTdElements[2].select_one('img')["alt"])
                    print("rstTbodyTrTdElements => ", 3, rstTbodyTrTdElements[3].getText().strip())
                    print("rstTbodyTrTdElements => ", 4, rstTbodyTrTdElements[4].getText().strip())
                    print("rstTbodyTrTdElements => ", 5, rstTbodyTrTdElements[5].getText().strip())
                    print("rstTbodyTrTdElements => ", 6, rstTbodyTrTdElements[6].getText().strip())

                    strYYYYMMDD = rstTbodyTrTdElements[0].getText().strip().replace(".","")

                    if len(strYYYYMMDD) < 3:
                        continue

                    strHHII = "1630"
                    strChangeText = (rstTbodyTrTdElements[2].select_one('img')["alt"]).strip()
                    strExchangeRate = rstTbodyTrTdElements[1].getText().strip().replace(",", "").zfill(1)
                    strChangeRate = rstTbodyTrTdElements[2].getText().strip().replace(",", "").zfill(1)
                    strCashSell = rstTbodyTrTdElements[3].getText().strip().replace(",", "").zfill(1)
                    strCashBuy = rstTbodyTrTdElements[4].getText().strip().replace(",","").zfill(1)
                    strSendMoney = rstTbodyTrTdElements[5].getText().strip().replace(",", "").replace('N/A', '').zfill(1)
                    strReceiveMoney = rstTbodyTrTdElements[6].getText().strip().replace(",", "").replace('N/A', '').zfill(1)

                    strChangeFlag = 'EVEN'
                    if strChangeText == '상승':
                        strChangeFlag = 'RASE'
                    elif strChangeText == '하락':
                        strChangeFlag = 'FALL'


                    sqlSelectStockIndex = "SELECT * FROM " + ConstTableName.NaverStockExchangeRateTable
                    sqlSelectStockIndex += " WHERE YYYYMMDD = %s "
                    sqlSelectStockIndex += " AND market_code = %s "

                    cursorStockFriends.execute(sqlSelectStockIndex, (strYYYYMMDD,listIndex))
                    intSelectedCount = cursorStockFriends.rowcount
                    print("intSelectedCount ==> ",sqlSelectStockIndex ,  strYYYYMMDD,listIndex ,  intSelectedCount)
                    if intSelectedCount > 0:
                        rstSelectDatas = cursorStockFriends.fetchone()
                        intInsertedID = int(rstSelectDatas.get('seq'))
                        dictUpdate = dict()
                        # dictUpdate['YYYYMMDD'] = strYYYYMMDD
                        # dictUpdate['HHII'] = strHHII
                        # dictUpdate['market_code'] = listIndex
                        dictUpdate['change_flag'] = strChangeFlag
                        dictUpdate['exchage_rete'] = strExchangeRate
                        dictUpdate['change_rate'] = strChangeRate
                        dictUpdate['cash_sell'] = strCashSell
                        dictUpdate['cash_buy'] = strCashBuy
                        dictUpdate['send_money'] = strSendMoney
                        dictUpdate['receive_money'] = strReceiveMoney

                        listFieldValues = list()

                        sqlUpdateItemTable = " UPDATE " + ConstTableName.NaverStockExchangeRateTable + " SET  "
                        sqlUpdateItemTable += " modify_date = NOW() "

                        for dictUpdatetKey, dictUpdateValue in dictUpdate.items():
                            sqlUpdateItemTable += ", " + dictUpdatetKey + " = %s"
                            listFieldValues.append(dictUpdateValue)

                        sqlUpdateItemTable += " WHERE YYYYMMDD = '"+strYYYYMMDD+"' "
                        sqlUpdateItemTable += " AND market_code = '" + listIndex + "' "

                        print("sqlUpdateItemTable ==> ", sqlUpdateItemTable, listFieldValues)

                        cursorStockFriends.execute(sqlUpdateItemTable, listFieldValues)
                        ResStockFriendsConnection.commit()


                    else:

                        dictInsert = dict()
                        dictInsert['YYYYMMDD'] = strYYYYMMDD
                        # dictInsert['HHII'] = strHHII
                        dictInsert['from_country_code'] = strFromCountryCode
                        dictInsert['to_country_code'] = strToCountryCode
                        dictInsert['market_code'] = listIndex
                        dictInsert['change_flag'] = strChangeFlag
                        dictInsert['exchage_rete'] = strExchangeRate
                        dictInsert['change_rate'] = strChangeRate
                        dictInsert['cash_sell'] = strCashSell
                        dictInsert['cash_buy'] = strCashBuy
                        dictInsert['send_money'] = strSendMoney
                        dictInsert['receive_money'] = strReceiveMoney

                        listFieldValues = list()
                        sqlInsertItemTable = " INSERT INTO " + ConstTableName.NaverStockExchangeRateTable + " SET  "
                        sqlInsertItemTable += " HHII = '1630' "

                        for dictInsertKey, dictInsertValue in dictInsert.items():
                            sqlInsertItemTable += ", " + dictInsertKey + " = %s"
                            listFieldValues.append(dictInsertValue)

                        print("sqlInsertItemTable ==> ", sqlInsertItemTable)
                        print("listFieldValues ==> ", listFieldValues)

                        cursorStockFriends.execute(sqlInsertItemTable, listFieldValues)
                        ResStockFriendsConnection.commit()
                        intInsertedID = cursorStockFriends.lastrowid


                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = listIndex
                dictSwitchData['data_2'] = strYYYYMMDD
                dictSwitchData['data_3'] = intNowPage
                dictSwitchData['data_4'] = intInsertedID
                StockSwitchTable.SwitchResultUpdateV2(logging, strProcessType, 'b', dictSwitchData)
                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "============================time.sleep(1) ")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " ============================time.sleep(1)")

                intNowPage += 1

                time.sleep(3)
                if bStopFlag == True:
                    print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "bStopFlag Break")
                    break




        driver.quit()  # 크롬 브라우저 닫기
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[listUpdateColumn][" + str(
            listUpdateColumn) + "]")


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
        # driver.quit()  # 크롬 브라우저 닫기
    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())  + "[ELSE]========================================================")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Finally END]========================================================")


if __name__ == '__main__':
    main()