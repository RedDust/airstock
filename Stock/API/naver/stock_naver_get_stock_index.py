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
        strProcessType = '010001'
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

        listIndexs.append('KOSPI')
        listIndexs.append('KOSDAQ')

        strCountryCode = 'kr'


        for listIndex in listIndexs:

            intNowPage = 1
            bStopFlag = False

            while True:
                RealtyCallUrl = 'https://finance.naver.com/sise/sise_index_day.naver?code=' + listIndex
                RealtyCallUrl += '&page=' + str(intNowPage)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[RealtyCallUrl]" + str(RealtyCallUrl))

                strResult = driver.get(RealtyCallUrl)  # 크롤링할 사이트 호출
                htmlSource = driver.page_source  # page_source 얻기

                soup = BeautifulSoup(htmlSource, "html.parser")  # get html
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[soup][" + str(soup)+"]")

                # [2. 코스피 / 코스닥 / 코넥스 조회 ]###################################################################################
                rstTbodyElement = soup.select_one('body > div > table.type_1 > tbody')
                rstNnaviTbodyElement = soup.select_one('body > div > table.Nnavi > tbody > tr > td.pgRR')

                rstTbodyTrElements = rstTbodyElement.select('tr')

                print("rstNnaviTbodyElement => " , rstNnaviTbodyElement)

                if rstNnaviTbodyElement is None:
                    bStopFlag = True

                for rstTbodyTrElement in rstTbodyTrElements:
                    # print("rstTbodyTrElement => ", rstTbodyTrElement)
                    rstTbodyTrTdElements = rstTbodyTrElement.select('td')

                    print("==================================================================" , len(rstTbodyTrTdElements))
                    intTdLoopCnt = 0
                    if len(rstTbodyTrTdElements) != 6:
                        continue

                    print("rstTbodyTrTdElements => ", 0, rstTbodyTrTdElements[0].getText().strip())
                    print("rstTbodyTrTdElements => ", 1, rstTbodyTrTdElements[1].getText().strip())
                    print("rstTbodyTrTdElements => ", 2, rstTbodyTrTdElements[2].getText().strip())
                    print("rstTbodyTrTdElements => ", 3, rstTbodyTrTdElements[3].getText().strip())
                    print("rstTbodyTrTdElements => ", 4, rstTbodyTrTdElements[4].getText().strip())
                    print("rstTbodyTrTdElements => ", 5, rstTbodyTrTdElements[5].getText().strip())

                    strYYYYMMDD = rstTbodyTrTdElements[0].getText().strip().replace(".","")

                    if len(strYYYYMMDD) < 3:
                        continue

                    strHHII = "1630"
                    strIndexPoint = rstTbodyTrTdElements[1].getText().strip().replace(",", "").zfill(1)
                    strChangePoint = rstTbodyTrTdElements[2].getText().strip().replace(",", "").zfill(1)
                    strChangePersent = rstTbodyTrTdElements[3].getText().strip().replace("%", "").zfill(1)
                    strTradeVolume = rstTbodyTrTdElements[4].getText().strip().replace(",","").zfill(1)
                    strTradeAmount = rstTbodyTrTdElements[5].getText().strip().replace(",", "").zfill(1)
                    floatChangePersent = float(strChangePersent)
                    strChangeFlag = 'EVEN'
                    if floatChangePersent > 0:
                        strChangeFlag = 'RASE'
                    elif floatChangePersent < 0:
                        strChangeFlag = 'FALL'

                    dictInsert = dict()
                    dictInsert['YYYYMMDD'] = strYYYYMMDD
                    # dictInsert['HHII'] = strHHII
                    dictInsert['country_code'] = strCountryCode
                    dictInsert['market_code'] = listIndex
                    dictInsert['change_flag'] = strChangeFlag
                    dictInsert['index_point'] = strIndexPoint
                    dictInsert['change_point'] = strChangePoint
                    dictInsert['change_rate'] = strChangePersent
                    dictInsert['trade_volume'] = strTradeVolume
                    dictInsert['trade_amount'] = strTradeAmount

                    sqlSelectStockIndex = "SELECT * FROM " + ConstTableName.NaverStockStockIndexTable
                    sqlSelectStockIndex += " WHERE YYYYMMDD = %s "
                    sqlSelectStockIndex += " AND market_code = %s "

                    cursorStockFriends.execute(sqlSelectStockIndex, (strYYYYMMDD,listIndex))
                    intSelectedCount = cursorStockFriends.rowcount
                    print("intSelectedCount ==> ",sqlSelectStockIndex ,  strYYYYMMDD,listIndex ,  intSelectedCount)
                    if intSelectedCount > 0:
                        continue

                    listFieldValues = list()
                    sqlInsertItemTable = " INSERT INTO " + ConstTableName.NaverStockStockIndexTable + " SET  "
                    sqlInsertItemTable += " HHII = '1630' "

                    for dictInsertKey, dictInsertValue in dictInsert.items():
                        sqlInsertItemTable += ", " + dictInsertKey + " = %s"
                        listFieldValues.append(dictInsertValue)

                    print("sqlInsertItemTable ==> ", sqlInsertItemTable)
                    print("listFieldValues ==> ", listFieldValues)

                    cursorStockFriends.execute(sqlInsertItemTable, listFieldValues)
                    ResStockFriendsConnection.commit()
                    intInsertedID = cursorStockFriends.lastrowid
                #
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
                time.sleep(5)

                if bStopFlag == True:
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