# 한국
# 2024-11-25 커밋
#https://www.data.go.kr/iim/api/selectAPIAcountView.do
#https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo

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

from Stock.CONFIG import ConstTableName

from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Lib.CustomException.QuitException import QuitException
from Stock.LIB.RDB import pyMysqlConnector
from Stock.API.koreaInvestment.package_koreainvest import dict_koreainvest_get_qoutes as getDictKoreainvestQoutes


def slave_thread(logging, dtNowStartTime, dtNowEndTime, rstSelectData, strItemTableName):

    import inspect as Isp
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
    from Stock.LIB.Functions.Table import ManufactureItemTable as MIT
    import Stock.API.koreaInvestment.Lib.kis_auth as ka
    import Stock.API.koreaInvestment.Lib.kis_domstk as kb
    import Stock.API.koreaInvestment.stock_koreainvest_get_price_slave as GetPrice

    try:

        # 토큰 발급
        ka.auth()


        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " slave_thread=>]")


        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rstSelectData=>]" + str(rstSelectData))
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " dtNowTime=>]" + str(dtNowStartTime) +" =>" + str(dtNowEndTime) )

        strBaseYYYY = str(dtNowEndTime.year).zfill(4)
        strBaseMM = str(dtNowEndTime.month).zfill(2)
        strBaseDD = str(dtNowEndTime.day).zfill(2)
        strBaseHH = str(dtNowEndTime.hour).zfill(2)
        strBaseII = str(dtNowEndTime.minute).zfill(2)
        strBaseSS = str(dtNowEndTime.second).zfill(2)

        strNowDate = strBaseYYYY + strBaseMM + strBaseDD
        strNowHHMM = strBaseHH + strBaseII

        strStartBaseHH = str(dtNowStartTime.hour).zfill(2)
        strStartBaseII = str(dtNowStartTime.minute).zfill(2)

        strEndBaseHH = str(dtNowEndTime.hour).zfill(2)
        strEndBaseII = str(dtNowEndTime.minute).zfill(2)

        strStartHHII = strStartBaseHH + strStartBaseII
        strEndHHII = strEndBaseHH + strEndBaseII


        # # DB 연결
        ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
        cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)

        KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
        cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)

        strDBSectorsName = rstSelectData.get('item_name')
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBSectorsName=====================================> [" + str(
                strDBSectorsName) + "]")


        strDBSequence = rstSelectData.get('seq')
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "] strDBSequence=>[" + str(strDBSequence) + "]")

        strDBItemCode = rstSelectData.get('item_code')
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "] strDBItemCode=> [" + str(
                strDBItemCode) + "]")

        strDBCountryCode = str(rstSelectData.get('country_code'))
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "] strDBCountryCode=> [" + str(
                strDBCountryCode) + "]")

        strDBUnitPrice = str(rstSelectData.get('unit_price'))
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "] strDBUnitPrice=> [" + str(
                strDBUnitPrice) + "]")

        strDBMultiplication = str(rstSelectData.get('multiplication'))
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "] strDBMultiplication=> [" + str(
                strDBMultiplication) + "]")

        # strStartHHMM = listTimeDatas[0][0]
        # strEndHHMM = listTimeDatas[0][1]


        # 전일 거래 내역
        sqlSelectBeforeDayItemTables = " SELECT * FROM " + strItemTableName + " WHERE HHII <= %s "
        sqlSelectBeforeDayItemTables += " AND YYYYMMDD < %s  "
        sqlSelectBeforeDayItemTables += " ORDER BY YYYYMMDD DESC , HHII DESC   "
        sqlSelectBeforeDayItemTables += " LIMIT 1  "

        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(
                sqlSelectBeforeDayItemTables) + "] strEndHHII=> [" + str(strEndHHII) + "]" + " strNowDate=> [" + str(
                strNowDate) + "]")

        cursorKoreaInvestItems.execute(sqlSelectBeforeDayItemTables, (strEndHHII, strNowDate))
        intAffectedCount = cursorKoreaInvestItems.rowcount
        if intAffectedCount > 0:
            rstSelectBeforeItemDatas = cursorKoreaInvestItems.fetchone()

            intBeforeDayVolumn = int(rstSelectBeforeItemDatas.get('acml_vol'))
            if intBeforeDayVolumn > 10000:
                # intBeforeDayTimevolumn = intBeforeDayVolumn + round(intBeforeDayVolumn / 10)
                intBeforeDayTimevolumn = intBeforeDayVolumn
            else:
                intBeforeDayTimevolumn = 100000
        else:
            intBeforeDayTimevolumn = 100000


        # 이전 거래 내역
        sqlSelectItemTables = " SELECT * FROM " + strItemTableName + " WHERE HHII = %s "
        # sqlSelectItemTables += " AND stck_hgpr = stck_prpr "
        sqlSelectItemTables += " AND YYYYMMDD = %s  "
        sqlSelectItemTables += " LIMIT 1 "
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(sqlSelectItemTables) + "] strStartHHII=> [" + str(strStartHHII) + "]" + " strNowDate=> [" + str(strNowDate) + "]")

        cursorKoreaInvestItems.execute(sqlSelectItemTables, (strStartHHII, strNowDate))
        intAffectedCount = cursorKoreaInvestItems.rowcount

        if intAffectedCount < 1:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        rstSelectItemDatas = cursorKoreaInvestItems.fetchone()
        if rstSelectItemDatas == None:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        b_acml_vol = strDBacml_vol = rstSelectItemDatas.get('acml_vol')
        b_strDBstck_prpr = strDBstck_prpr = rstSelectItemDatas.get('stck_prpr')

        b_frgn_ntby_qty = rstSelectItemDatas.get('frgn_ntby_qty')
        b_pgtr_ntby_qty = rstSelectItemDatas.get('pgtr_ntby_qty')
        b_stck_hgpr = rstSelectItemDatas.get('stck_hgpr')

        # print("acml_vol=>", strDBacml_vol)
        intDBacml_vol = int(strDBacml_vol)
        # logging.info(
        #     SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(
        #         strDBSectorsName) + "] intDBacml_vol=> [" + str(intDBacml_vol) + "]" + "] intBeforeDayTimevolumn=> [" + str(intBeforeDayTimevolumn) + "]")

        intb_acml_vol = int(b_acml_vol)
        if intb_acml_vol < intBeforeDayTimevolumn:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "]")  # 예외를 발생시킴

        # 100000 - 100000 * (intListCount / intTimeLoop)

        # intCalData = int(100000 * round(intTimeLoop / intListCount, 5))
        # print("intCalData->", intTimeLoop, "/", intListCount, "=> ", intCalData)
        #
        # if intCalData < 10000:
        #     intCalData = 10000
        #
        # if intDBacml_vol < 100000:
        #     print("-------------------------------------------")
        #     continue

        # intDBAddacml_vol = round(intDBacml_vol / 10)
        # intDBAddacml_vol = round(intDBacml_vol / 2)
        # intDBAddacml_vol = round(intDBacml_vol / 5)
        # intDBAddacml_vol = round(intDBacml_vol / 3)
        # intDBAddacml_vol = round(intDBacml_vol / 4)
        #
        #

        # 현재 거래 내역
        sqlSelectItemTables = " SELECT * FROM " + strItemTableName + " "
        sqlSelectItemTables += "  WHERE YYYYMMDD = %s  "
        sqlSelectItemTables += " AND HHII = %s "
        sqlSelectItemTables += " LIMIT 1 "


        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " sqlSelectItemTables=> [" + str(
            strItemTableName) + "][" + str(sqlSelectItemTables) + "]")

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strNowDate=> [" + str(
            strItemTableName) + "][" + str(strNowDate) + "]")

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strEndHHII=> [" + str(
            strItemTableName) + "][" + str(strEndHHII) + "]")


        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBstck_prpr=> [" + str(
            strItemTableName) + "][" + str(strDBstck_prpr) + "]")


        cursorKoreaInvestItems.execute(sqlSelectItemTables, (strNowDate, strEndHHII))
        intSecAffectedCount = cursorKoreaInvestItems.rowcount

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intSecAffectedCount=> [" + str(
            strItemTableName) + "][" + str(intSecAffectedCount) + "]")

        if intSecAffectedCount < 1:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "]")  # 예외를 발생시킴

        rst0300SelectItemDatas = cursorKoreaInvestItems.fetchone()
        a_frgn_ntby_qty = rst0300SelectItemDatas.get('frgn_ntby_qty')
        a_pgtr_ntby_qty = rst0300SelectItemDatas.get('pgtr_ntby_qty')
        a_acml_vol = rst0300SelectItemDatas.get('acml_vol')
        a_stck_hgpr = rst0300SelectItemDatas.get('stck_hgpr')
        a_stck_prpr = rst0300SelectItemDatas.get('stck_prpr')
        a_prdy_vrss_sign= rst0300SelectItemDatas.get('prdy_vrss_sign')


        # if a_stck_prpr < strDBstck_prpr:
        #     raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strDBstck_prpr[" + str(strDBstck_prpr) + "]")  # 예외를 발생시킴

        if a_prdy_vrss_sign != '2':
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strDBstck_prpr[" + str(
                strDBstck_prpr) + "]")  # 예외를 발생시킴

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " b_pgtr_ntby_qty=> [" + str(
            b_pgtr_ntby_qty) + "], a_pgtr_ntby_qty=>[" + str(
            a_pgtr_ntby_qty) + "]")

        intSubpgtr_ntby_qty = int(b_pgtr_ntby_qty) - int(a_pgtr_ntby_qty)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intSubpgtr_ntby_qty=>  [" + str(
            type(intSubpgtr_ntby_qty)) + "][" + str(intSubpgtr_ntby_qty) + "]")


        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intDBacml_vol=>  [" + str(
            type(intDBacml_vol)) + "][" + str(intDBacml_vol) + "]")

        if intDBacml_vol < 1:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intRecommendPoint=> [" + str(
                strItemTableName) + "][" + str(type(intDBacml_vol)) + "][" + str(
                intDBacml_vol) + "]")
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "]")  # 예외를 발생시킴

        inta_acml_vol = int(a_acml_vol)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " inta_acml_vol=> [" + str(
            type(inta_acml_vol)) + "] [" + str(inta_acml_vol) + "]" )
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intDBacml_vol=> [" + str(
            type(intDBacml_vol)) + "] [" + str(intDBacml_vol) + "]" )

        intTemp1 = inta_acml_vol - intDBacml_vol
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intTemp1=> [" + str(
            type(intTemp1)) + "] [" + str(intTemp1) + "]" )

        floatTempValue = float( intTemp1 / intDBacml_vol)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " floatTempValue=> [" + str(
            type(floatTempValue)) + "][" + str(floatTempValue) + "]" )


        floatRecommendPoint = round(floatTempValue * 100)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " floatRecommendPoint=> [" + str(
            strItemTableName) + "][" + str(type(floatRecommendPoint)) + "][" + str(floatRecommendPoint) + "]")


        strRecommendPoint = str(floatRecommendPoint)
        intRecommendPoint = int(floatRecommendPoint)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " floatRecommendPoint=> [" + str(
            strItemTableName) + "][" + str(type(floatRecommendPoint)) + "][" + str(floatRecommendPoint) + "]")

        if intRecommendPoint < 10:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intRecommendPoint=> [" + str(
                strItemTableName) + "][" + str(type(intRecommendPoint)) + "][" + str(
                intRecommendPoint) + "]")
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "]")  # 예외를 발생시킴

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strRecommendPoint=> [" + str(
            strRecommendPoint) + "][" + str(strRecommendPoint) + "]")

        sqlSelectRecommendationCnt = "SELECT * FROM stockfriends_koreainvest_recommendation_5 WHERE YYYYMMDD = %s AND item_code =%s"
        cursorStockFriends.execute(sqlSelectRecommendationCnt, (strNowDate, strDBItemCode))
        intRecommendDailyCount = cursorStockFriends.rowcount
        strRecommendDailyCount = str(intRecommendDailyCount)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strRecommendDailyCount=> [" + str(
            type(strRecommendDailyCount)) + "][" + str(strRecommendDailyCount) + "]")


        sqlSelectRecommendation = "SELECT * FROM stockfriends_koreainvest_recommendation_5 WHERE YYYYMMDD = %s AND ENDHHII = %s AND item_code =%s"


        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strNowDate=> [" + str(
            type(strNowDate)) + "][" + str(strNowDate) + "]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strEndHHII=> [" + str(
            type(strEndHHII)) + "][" + str(strEndHHII) + "]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDBItemCode=> [" + str(
            type(strDBItemCode)) + "][" + str(strDBItemCode) + "]")



        cursorStockFriends.execute(sqlSelectRecommendation, (strNowDate, strEndHHII, strDBItemCode))
        intRecommendCount = cursorStockFriends.rowcount

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " intRecommendCount=> [" + str(
            type(intRecommendCount)) + "][" + str(intRecommendCount) + "]")


        if intRecommendCount < 1:

            dictKoreaInvestNowQoutes = getDictKoreainvestQoutes(strDBItemCode)

            if isinstance(dictKoreaInvestNowQoutes, dict) != True:
                raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[" + str(strDBSectorsName) + "]")  # 예외를 발생시킴


            strSellQoutes = str(0)
            strSellVolumn = str(0)
            strjsonSellQoute = str(0)
            intSellSumAmount = str(0)

            strBuyQoutes = str(0)
            strBuyVolumn = str(0)
            strjsonBuyQoute = str(0)
            intBuySumAmount = str(0)

            strSellQoutes = str(dictKoreaInvestNowQoutes['sell'][0]['qoute'])
            strSellVolumn = str(dictKoreaInvestNowQoutes['sell'][0]['volumn'])
            strjsonSellQoute = str(json.dumps(dictKoreaInvestNowQoutes['sell']))
            intSellSumAmount = int(dictKoreaInvestNowQoutes['sell'][0]['amount'])

            strBuyQoutes = str(dictKoreaInvestNowQoutes['buy'][0]['qoute'])
            strBuyVolumn = str(dictKoreaInvestNowQoutes['buy'][0]['volumn'])
            strjsonBuyQoute = str(json.dumps(dictKoreaInvestNowQoutes['buy']))
            intBuySumAmount = int(dictKoreaInvestNowQoutes['buy'][0]['amount'])

            if intSellSumAmount < 100000:
                intSellSumAmount += int(dictKoreaInvestNowQoutes['sell'][1]['amount'])
                intSellQoutes = int(strSellQoutes) + int(dictKoreaInvestNowQoutes['sell'][1]['qoute'])
                intSellVolumn = int(strSellVolumn) + int(dictKoreaInvestNowQoutes['sell'][1]['volumn'])
                strSellQoutes = str(intSellQoutes)
                strSellVolumn = str(intSellVolumn)

                # raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

            if intBuySumAmount < 100000:
                intBuySumAmount += int(dictKoreaInvestNowQoutes['buy'][1]['amount'])
                intBuyQoutes = int(strBuyQoutes) + int(dictKoreaInvestNowQoutes['buy'][1]['qoute'])
                intBuyVolumn = int(strBuyVolumn) + int(dictKoreaInvestNowQoutes['buy'][1]['volumn'])
                strBuyQoutes = str(intBuyQoutes)
                strBuyVolumn = str(intBuyVolumn)


                # raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

            sqlInsertRecommendation = " INSERT INTO stockfriends_koreainvest_recommendation_5 SET "
            sqlInsertRecommendation += " YYYYMMDD =%s "
            sqlInsertRecommendation += " , STARTHHII =%s "
            sqlInsertRecommendation += " , ENDHHII =%s "
            sqlInsertRecommendation += " , recommend_point =%s "
            sqlInsertRecommendation += " , item_code =%s "
            sqlInsertRecommendation += " , item_name =%s "
            sqlInsertRecommendation += " , price =%s "
            sqlInsertRecommendation += " , b_price =%s "
            sqlInsertRecommendation += " , max_price =%s "
            sqlInsertRecommendation += " , b_frgn_ntby_qty =%s "
            sqlInsertRecommendation += " , a_frgn_ntby_qty =%s "
            sqlInsertRecommendation += " , b_pgtr_ntby_qty =%s "
            sqlInsertRecommendation += " , a_pgtr_ntby_qty =%s "
            sqlInsertRecommendation += " , b_acml_vol =%s "
            sqlInsertRecommendation += " , a_acml_vol =%s "
            sqlInsertRecommendation += " , sell_qoutes =%s "
            sqlInsertRecommendation += " , sell_volumn =%s "
            sqlInsertRecommendation += " , sell_qoutes_json =%s "
            sqlInsertRecommendation += " , buy_qoutes =%s "
            sqlInsertRecommendation += " , buy_amount =%s "
            sqlInsertRecommendation += " , buy_qoutes_json =%s "
            sqlInsertRecommendation += " , daily_cnt =%s "

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + " stockfriends_koreainvest_recommendation_5 strRecommendPoint=> [" + str(
                strRecommendPoint) + "][" + str(strRecommendPoint) + "]")

            listArgument = []
            listArgument.append(strNowDate)
            listArgument.append(strStartHHII)
            listArgument.append(strEndHHII)
            listArgument.append(strRecommendPoint)
            listArgument.append(strDBItemCode)
            listArgument.append(strDBSectorsName)
            listArgument.append(a_stck_prpr)
            listArgument.append(b_strDBstck_prpr)
            listArgument.append(a_stck_hgpr)
            listArgument.append(b_frgn_ntby_qty)
            listArgument.append(a_frgn_ntby_qty)
            listArgument.append(b_pgtr_ntby_qty)
            listArgument.append(a_pgtr_ntby_qty)
            listArgument.append(b_acml_vol)
            listArgument.append(a_acml_vol)
            listArgument.append(strSellQoutes)
            listArgument.append(strSellVolumn)
            listArgument.append(strjsonSellQoute)
            listArgument.append(strBuyQoutes)
            listArgument.append(strBuyVolumn)
            listArgument.append(strjsonBuyQoute)
            listArgument.append(strRecommendDailyCount)

            cursorStockFriends.execute(sqlInsertRecommendation, (listArgument))
            ResStockFriendsConnection.commit()




    except Exception as e:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " Exception=>")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " err_msg=>" + err_msg)

    except QuitException as e:


        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        # print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " QuitException=> [" + str( err_msg) + "]")
        # print(type(e))

        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " QuitException=> [" + str( e) + "]")

    else:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " ELSE=>")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + " Finally END=>")
        # time.sleep(2)
        # ResRealEstateConnection.close()

