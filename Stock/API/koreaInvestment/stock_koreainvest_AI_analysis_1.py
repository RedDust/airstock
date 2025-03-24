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
import pandas as pd

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
    import Stock.API.koreaInvestment.Lib.kis_auth as ka
    import Stock.API.koreaInvestment.Lib.kis_domstk as kb

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
        strNowYYYYMMDD = strNowDate
        strNowHHMM = strBaseHH + strBaseII

        # strNowYYYYMMDD = strNowDate = "20241204"


        # 초기값
        strProcessType = '020202'
        strDBSequence = '0'
        strDBSectorsName = ''
        intItemLoop = 0
        strItemDatabaseName='koreainvest'

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

        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)


        KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
        cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)

        sqlSelectItems = "SELECT * FROM " +ConstTableName.NaverStockItemTable +" WHERE state != '99' "
        sqlSelectItems += " AND seq > %s "
        sqlSelectItems += " AND market_code in ('KOSDAQ' , 'KOSPI','ETC') "
        # sqlSelectItems += " AND seq >= 1103 "
        sqlSelectItems += " ORDER BY seq ASC "
        # sqlSelectItems += " LIMIT 100 "
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

                strItemTableName = MIT.GetItemTableName(strDBItemCode, strDBCountryCode, strItemDatabaseName)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strItemTableName=> [" + str(len(
                    strItemTableName)) + "][" + str(
                    strItemTableName) + "]")

                if len(strItemTableName) < 1:
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


                #오늘 2시 30분 까지
                sqlSelectItemTables = " SELECT * FROM " + strItemTableName + " WHERE HHII = '1430' "
                # sqlSelectItemTables += " AND stck_hgpr = stck_prpr "
                sqlSelectItemTables += " AND YYYYMMDD = %s  "
                cursorKoreaInvestItems.execute(sqlSelectItemTables, (strNowDate))
                intAffectedCount = cursorKoreaInvestItems.rowcount

                if intAffectedCount > 0:
                    rstSelectItemDatas = cursorKoreaInvestItems.fetchone()
                    if rstSelectItemDatas == None:
                        raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                    b_acml_vol= strDBacml_vol = rstSelectItemDatas.get('acml_vol')
                    strDBstck_prpr = rstSelectItemDatas.get('stck_prpr')

                    b_frgn_ntby_qty = rstSelectItemDatas.get('frgn_ntby_qty')
                    b_pgtr_ntby_qty = rstSelectItemDatas.get('pgtr_ntby_qty')
                    b_stck_hgpr = rstSelectItemDatas.get('stck_hgpr')


                    # print("acml_vol=>", strDBacml_vol)
                    intDBacml_vol = int(strDBacml_vol)
                    # intDBAddacml_vol = round(intDBacml_vol / 10)
                    # intDBAddacml_vol = round(intDBacml_vol / 2)
                    # intDBAddacml_vol = round(intDBacml_vol / 5)
                    intDBAddacml_vol = round(intDBacml_vol / 4)


                    strCalDBacml_vol = str(intDBacml_vol + intDBAddacml_vol)  # +10%

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strCalDBacml_vol=> [" + str(
                        intDBacml_vol) + "][" + str(strCalDBacml_vol) + "]")

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strCalDBacml_vol=> ["+ str(
                    strItemTableName)+"][" + str(
                        strCalDBacml_vol) + "]=["+str(intDBacml_vol)+"]+[" + str(intDBAddacml_vol) + "]")

                    # print("strCalDBacml_vol => ", strCalDBacml_vol, "stck_prpr=>", strDBItemCode)
                    sqlSelectItemTables = " SELECT * FROM " + strItemTableName + " "
                    sqlSelectItemTables += "  WHERE YYYYMMDD = %s  "
                    sqlSelectItemTables += " AND HHII = '1500' "

                    # sqlSelectItemTables += " CAST( acml_vol AS UNSIGNED) >= %s "
                    # sqlSelectItemTables += " acml_vol >= %s "
                    sqlSelectItemTables += " AND stck_prpr > %s "
                    # sqlSelectItemTables += " AND frgn_ntby_qty > 0 "
                    # sqlSelectItemTables += " AND pgtr_ntby_qty > %s "
                    sqlSelectItemTables += " AND prdy_vrss_sign = '2' "



                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " sqlSelectItemTables=> ["+ str(
                    strItemTableName)+"][" + str(sqlSelectItemTables) + "]")

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strCalDBacml_vol=> ["+ str(
                    strItemTableName)+"][" + str(strCalDBacml_vol) + "]")

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBstck_prpr=> ["+ str(
                    strItemTableName)+"][" + str(strDBstck_prpr) + "]")

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strNowDate=> ["+ str(
                    strItemTableName)+"][" + str(strNowDate) + "]")

                    cursorKoreaInvestItems.execute(sqlSelectItemTables, (strNowDate , strDBstck_prpr))
                    intSecAffectedCount = cursorKoreaInvestItems.rowcount


                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intSecAffectedCount=> ["+ str(
                    strItemTableName)+"][" + str(intSecAffectedCount) + "]")
                    if intSecAffectedCount < 1:
                        continue

                    rst0300SelectItemDatas = cursorKoreaInvestItems.fetchone()
                    a_frgn_ntby_qty = rst0300SelectItemDatas.get('frgn_ntby_qty')
                    a_pgtr_ntby_qty = rst0300SelectItemDatas.get('pgtr_ntby_qty')
                    a_acml_vol = rst0300SelectItemDatas.get('acml_vol')
                    a_stck_hgpr = rst0300SelectItemDatas.get('stck_hgpr')
                    a_stck_prpr = rstSelectItemDatas.get('stck_prpr')

                    intSubpgtr_ntby_qty = int(b_pgtr_ntby_qty) - int(a_pgtr_ntby_qty)


                    floatRecommendPoint = round((float(int(a_acml_vol) / intDBacml_vol) -1 ) * 100)
                    strRecommendPoint = str(floatRecommendPoint)
                    intRecommendPoint = int(floatRecommendPoint)

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " floatRecommendPoint=> [" + str(
                        strItemTableName) + "][" + str(type(floatRecommendPoint)) + "][" + str(floatRecommendPoint) + "]")

                    if intRecommendPoint < 20:
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intRecommendPoint=> [" + str(
                            strItemTableName) + "][" + str(type(intRecommendPoint)) + "][" + str(
                            intRecommendPoint) + "]")
                        continue

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strRecommendPoint=> [" + str(
                        strRecommendPoint) + "][" + str(strRecommendPoint) + "]")

                    sqlSelectRecommendation = "SELECT * FROM stockfriends_koreainvest_recommendation_2 WHERE YYYYMMDD = %s AND sectors_code =%s"
                    cursorStockFriends.execute(sqlSelectRecommendation,(strNowYYYYMMDD, strDBItemCode))
                    intRecommendCount = cursorStockFriends.rowcount

                    if intRecommendCount < 1:
                        sqlInsertRecommendation = " INSERT INTO stockfriends_koreainvest_recommendation_2 SET "
                        sqlInsertRecommendation += " YYYYMMDD =%s "
                        sqlInsertRecommendation += " , HHII =%s "
                        sqlInsertRecommendation += " , recommend_point =%s "
                        sqlInsertRecommendation += " , sectors_code =%s "
                        sqlInsertRecommendation += " , sectors_name =%s "
                        sqlInsertRecommendation += " , price =%s "
                        sqlInsertRecommendation += " , max_price =%s "
                        sqlInsertRecommendation += " , b_frgn_ntby_qty =%s "
                        sqlInsertRecommendation += " , a_frgn_ntby_qty =%s "
                        sqlInsertRecommendation += " , b_pgtr_ntby_qty =%s "
                        sqlInsertRecommendation += " , a_pgtr_ntby_qty =%s "
                        sqlInsertRecommendation += " , b_acml_vol =%s "
                        sqlInsertRecommendation += " , a_acml_vol =%s "

                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " stockfriends_koreainvest_recommendation_2 strRecommendPoint=> [" + str(
                            strRecommendPoint) + "][" + str(strRecommendPoint) + "]")

                        cursorStockFriends.execute(sqlInsertRecommendation, (strNowYYYYMMDD,strNowHHMM, strRecommendPoint, strDBItemCode,strDBSectorsName,a_stck_prpr,a_stck_hgpr,b_frgn_ntby_qty,a_frgn_ntby_qty,b_pgtr_ntby_qty,a_pgtr_ntby_qty,b_acml_vol,a_acml_vol))
                        ResStockFriendsConnection.commit()

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = strNowDate
                dictSwitchData['data_2'] = strDBSequence
                dictSwitchData['data_3'] = strDBSectorsName
                dictSwitchData['data_4'] = intItemLoop
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