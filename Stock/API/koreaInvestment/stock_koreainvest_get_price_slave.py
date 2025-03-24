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
from datetime import datetime as DateTime, timedelta as TimeDelta
from Stock.LIB.Functions.Switch import StockSwitchTable
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Stock.LIB.Functions.Table import ManufactureItemTable as MIT
import Stock.API.koreaInvestment.Lib.kis_auth as ka
import Stock.API.koreaInvestment.Lib.kis_domstk as kb
import Stock.API.koreaInvestment.stock_koreainvest_get_price_slave as GetPrice




def child_thread(logging, StockSwitchTable, strDBItemCode, dtNow, strItemTableName):

    try:
        strItemDatabaseName='koreainvest'
        strProcessType = '020111'
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


        KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
        cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)

        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[CRONTAB START : "+strDBItemCode+" ]")

        if len(strItemTableName) < 1:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


        sqlSelectItems = " SELECT * FROM " + strItemTableName
        sqlSelectItems += " WHERE  YYYYMMDD = %s "
        sqlSelectItems += " AND  HHII = %s "
        cursorKoreaInvestItems.execute(sqlSelectItems , (strNowYYYYMMDD, strNowHHMM))
        intAffectedCount = cursorKoreaInvestItems.rowcount


        if intAffectedCount < 1:
            # [국내주식] 기본시세 > 주식현재가 시세 (종목번호 6자리)
            rt_data = kb.get_inquire_price(itm_no=strDBItemCode)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rt_data => [" + str(len(
                rt_data)) + "][" + str(rt_data) + "]")

            dictRtDatas = pd.DataFrame(rt_data)

            # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " dictRtDatas => [" + str(len(
            #     dictRtDatas)) + "][" + str(dictRtDatas) + "]")

            dictValues = dictRtDatas.to_dict()


            sqlInsertItems = " INSERT INTO " + strItemTableName + " SET "
            sqlInsertItems += " YYYYMMDD = '" + strNowYYYYMMDD + "' "
            sqlInsertItems += " , HHII = '" + strNowHHMM + "' "
            dictKoreaInvestDatas = dict()

            for dictkey, dictValue in dictValues.items():
                dictKoreaInvestDatas[dictkey] = dictValue[0]

                if dictkey != 'mang_issu_cls_code':
                    sqlInsertItems += "," + dictkey + "  = '" + str(dictValue[0]) + "'"

            cursorKoreaInvestItems.execute(sqlInsertItems)
            KoreaInvestItemsConnection.commit()

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_3'] = strDBItemCode
        StockSwitchTable.SwitchResultUpdateV3(logging, strProcessType, dtNow, 'c', dictSwitchData)


    except Exception as e:

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Exception => [" + str(len(
            strDBItemCode)) + "][" + str(strDBItemCode) + "]")

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")


    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException => [" + str(len(
            strDBItemCode)) + "][" + str(strDBItemCode) + "]")

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")



    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END => [" + str(len(
            strDBItemCode)) + "][" + str(strDBItemCode) + "]")

    finally:
        print("Finally END")
        # ResRealEstateConnection.close()




def child_Process(strDBItemCode, strItemTableName):


    try:
        strItemDatabaseName='koreainvest'
        strProcessType = '020111'


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
    #
        strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
        LogPath = 'Stock/'+strAddLogPath+'/'+ strProcessType + "_" + strDBItemCode

        setLogger = ULF.setLogFileV2(logging, LogPath)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[CRONTAB START : =====================================]")

        KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
        cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)

        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[CRONTAB START : "+strDBItemCode+" ]")

        if len(strItemTableName) < 1:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


        sqlSelectItems = " SELECT * FROM " + strItemTableName
        sqlSelectItems += " WHERE  YYYYMMDD = %s "
        sqlSelectItems += " AND  HHII = %s "
        cursorKoreaInvestItems.execute(sqlSelectItems , (strNowYYYYMMDD, strNowHHMM))
        intAffectedCount = cursorKoreaInvestItems.rowcount


        if intAffectedCount < 1:
            # [국내주식] 기본시세 > 주식현재가 시세 (종목번호 6자리)

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " INSERT strDBItemCode=> [" + str(len(
                strDBItemCode)) + "][" + str( strDBItemCode) + "]")

            rt_data = kb.get_inquire_price(itm_no=strDBItemCode)
            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), rt_data)  # 현재가, 전일대비

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " rt_data => [" + str(len(
                rt_data)) + "][" + str(rt_data) + "]")

            dictRtDatas = pd.DataFrame(rt_data)

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " dictRtDatas => [" + str(len(
                dictRtDatas)) + "][" + str(dictRtDatas) + "]")


            dictValues = dictRtDatas.to_dict()

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " dictValues => [" + str(len(
                dictValues)) + "][" + str(dictValues) + "]")

            sqlInsertItems = " INSERT INTO " + strItemTableName + " SET "
            sqlInsertItems += " YYYYMMDD = '" + strNowYYYYMMDD + "' "
            sqlInsertItems += " , HHII = '" + strNowHHMM + "' "
            dictKoreaInvestDatas = dict()

            for dictkey, dictValue in dictValues.items():
                dictKoreaInvestDatas[dictkey] = dictValue[0]
                sqlInsertItems += "," + dictkey + "  = '" + str(dictValue[0]) + "'"

            cursorKoreaInvestItems.execute(sqlInsertItems)
            # KoreaInvestItemsConnection.commit()

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_3'] = strDBItemCode
        StockSwitchTable.SwitchResultUpdateV3(logging, strProcessType, dtNow, 'c', dictSwitchData)


    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_3'] = strDBItemCode
        StockSwitchTable.SwitchResultUpdateV3(logging, strProcessType, dtNow, 'd', dictSwitchData)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " Exception => [" + str(len(
            strDBItemCode)) + "][" + str(strDBItemCode) + "]")

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            e)) + "][" + str(e) + "]")


    except QuitException as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " QuitException => [" + str(len(
            strDBItemCode)) + "][" + str(strDBItemCode) + "]")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_3'] = strDBItemCode
        StockSwitchTable.SwitchResultUpdateV3(logging, strProcessType, dtNow, 'd', dictSwitchData)

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " err_msg=> [" + str(len(
            err_msg)) + "][" + str(err_msg) + "]")
        print(e)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " e=> [" + str(len(
            e)) + "][" + str(e) + "]")



    else:
        print("========================= SUCCESS END")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " SUCCESS END => [" + str(len(
            strDBItemCode)) + "][" + str(strDBItemCode) + "]")

    finally:
        print("Finally END")
        # ResRealEstateConnection.close()
