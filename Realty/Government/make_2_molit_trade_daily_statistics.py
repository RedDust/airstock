import sys
sys.path.append("D:/PythonProjects/airstock")

# https://www.data.go.kr/iim/api/selectAPIAcountView.do
#국토교통부_단독/다가구 매매 실거래 자료
#SERVICE URL https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15058022
#[국토교통부_단독/다가구 매매 실거래 자료]API http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSHTrade

#ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable = 'kt_realty_gov_code_info'
#ConstRealEstateTable_GOV.MolitHouseRealTradeMasterTable = 'kt_realty_molit_house_real_trade_master'
#ConstRealEstateTable_GOV.MolitHouseRealTradeCancelTable = 'kt_realty_molit_house_real_trade_master_cancel'

import urllib.request
import json
import pymysql
import traceback
import time , datetime
import re
import pandas as pd
import requests
from pandas.io.json import json_normalize
from Realty.Government.Init import init_conf
from Lib.RDB import pyMysqlConnector
from dateutil.relativedelta import relativedelta

from Init.Functions.Logs import GetLogDef

from Realty.Government.Const import ConstRealEstateTable_GOV

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from bs4 import BeautifulSoup
import xml
import xml.etree.ElementTree as ET

from Lib.CustomException import QuitException

def main():

    try:

        # 60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
        # 1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
        # 거래 신고 30일 + 취소 신고 +30일
        stToday = DateTime.today()

        nInsertedCount = 0
        nUpdateCount = 0

        # 서울 부동산 실거래가 데이터 - 매매
        strProcessType = '035112'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'

        nProcessCount = 0
        GOVMoltyAddressSequence = '0'
        intLoopStart = 0
        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise Exception(GetLogDef.lineno(__file__) + 'strResult => ' + strResult)  # 예외를 발생시킴

        if strResult == '10':
            raise Exception(GetLogDef.lineno(__file__) + 'It is currently in operation. => ' + strResult)  # 예외를 발생시킴


        strTradeSequence = str(rstResult.get('data_1'))


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        # # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        sqlSelectMOLIT = " SELECT * FROM " + ConstRealEstateTable_GOV.MolitRealTradeMasterTable + " "
        sqlSelectMOLIT += " WHERE seq > %s "
        sqlSelectMOLIT += " ORDER BY seq ASC "
        # sqlSelectMOLIT += " LIMIT 100000 "

        cursorRealEstate.execute(sqlSelectMOLIT , strTradeSequence )
        intMolitCount = cursorRealEstate.rowcount
        print(GetLogDef.lineno(__file__), "intMolitCount", intMolitCount)

        rstSelectDatas = cursorRealEstate.fetchall()

        for rstSelectData in rstSelectDatas:
            strDBTradeSequence = str(rstSelectData.get('seq'))

            strDBDEAL_YMD = str(rstSelectData.get('DEAL_YMD'))
            strDBSIDO_CD = str(rstSelectData.get('SIDO_CD'))
            strDBSIDO_NM = str(rstSelectData.get('SIDO_NM'))
            strDBSGG_CD = str(rstSelectData.get('SGG_CD'))
            strDBSGG_NM = str(rstSelectData.get('SGG_NM'))
            strDBBJDONG_CD = str(rstSelectData.get('BJDONG_CD'))
            strDBBJDONG_NM = str(rstSelectData.get('BJDONG_NM'))
            strDBHOUSE_TYPE = str(rstSelectData.get('HOUSE_TYPE'))
            strDBACC_YEAR = str(rstSelectData.get('ACC_YEAR'))



            listDBTOT_AREA = str(rstSelectData.get('TOT_AREA')).split('.')
            print(GetLogDef.lineno(__file__), "listDBTOT_AREA", listDBTOT_AREA)

            strDBFLOOR = str(rstSelectData.get('FLOOR'))

            strDBOBJ_AMT= str(rstSelectData.get('OBJ_AMT'))



            strDBREGISTER_YMD = str(rstSelectData.get('REGISTER_YMD'))
            strDBstate = str(rstSelectData.get('state'))

            strDBTOT_AREA = listDBTOT_AREA[0]
            if len(strDBTOT_AREA) < 1:
                strDBTOT_AREA =str(rstSelectData.get('TOT_AREA'))



            intCancelCount = 0
            intRegisterCount = 0
            intTradeCount = 1
            intCancelMoney = 0
            intRegisterMoney = 0

            intTradeMoney = int(strDBOBJ_AMT)


            if len(strDBREGISTER_YMD) > 1:
                intRegisterCount = 1
                intRegisterMoney = intTradeMoney

            if strDBstate == '10':
                intCancelCount = 1
                intCancelMoney = intTradeMoney

            strAdminSection = strDBSIDO_NM + " " + strDBSGG_NM + " " + strDBBJDONG_NM
            strBaseYYYY = (strDBDEAL_YMD[0:4])
            strBaseMM = (strDBDEAL_YMD[4:6])
            strBaseDD = (strDBDEAL_YMD[6:8])
            strBaseYYYYMM = strBaseYYYY + strBaseMM
            strBaseYYYYMMDD = strBaseYYYYMM + strBaseDD

            dBaseIssueDatetime = datetime.date(int(strBaseYYYY), int(strBaseMM), int(strBaseDD))

            strBaseYYYYMMDDWeekDay = dBaseIssueDatetime.weekday()

            YYYYWEEK = str(dBaseIssueDatetime.isocalendar().year) + str(dBaseIssueDatetime.isocalendar().week).zfill(2)
            YYYYWEEKDAY = str(dBaseIssueDatetime.isocalendar().weekday)

            strUniqueKey = YYYYWEEK + "_" + strDBSIDO_CD + strDBSGG_CD + strDBBJDONG_CD
            strUniqueKey += "_" + strDBHOUSE_TYPE + "_" + strDBTOT_AREA

            strNewPartTable = ConstRealEstateTable_GOV.MolitRealTradeBasicAPTStatisticsMasterTable + "_" + strDBSIDO_CD

            qryCheckTable = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA='"+ConstRealEstateTable_GOV.RealtyDatabase+"' AND TABLE_NAME='"+strNewPartTable+"'"
            cursorRealEstate.execute(qryCheckTable)

            print(GetLogDef.lineno(__file__), "qryCheckTable > ", qryCheckTable)

            results = cursorRealEstate.rowcount

            print(GetLogDef.lineno(__file__), "results > ", results)

            if results < 1:
                qryCreateBackupTable = "create table " + strNewPartTable + " LIKE " + ConstRealEstateTable_GOV.MolitRealTradeBasicAPTStatisticsMasterTable
                print(GetLogDef.lineno(__file__), "qryCreateBackupTable > ", qryCreateBackupTable)
                cursorRealEstate.execute(qryCreateBackupTable)





            sqlSelectStatistics = " SELECT * FROM " + strNewPartTable + " "
            sqlSelectStatistics += " WHERE unique_key = %s "

            cursorRealEstate.execute(sqlSelectStatistics, strUniqueKey)
            intMolitStaticCount = cursorRealEstate.rowcount

            if intMolitStaticCount > 0:

                sqlModityStatistics  = " UPDATE " + strNewPartTable + " SET "
                sqlModityStatistics += " APT_TRADE_CNT = APT_TRADE_CNT + '" + str(intTradeCount) + "'  ,  "
                sqlModityStatistics += " APT_TRADE_MONEY = APT_TRADE_MONEY + '" + str(intTradeMoney) + "'  ,  "
                sqlModityStatistics += " APT_TRADE_CANCEL_CNT = APT_TRADE_CANCEL_CNT + '" + str(intCancelCount) + "' ,  "
                sqlModityStatistics += " APT_TRADE_CANCEL_MONEY = APT_TRADE_CANCEL_MONEY + '" + str(intCancelMoney) + "' ,  "
                sqlModityStatistics += " APT_TRADE_REGISTER_CNT = APT_TRADE_REGISTER_CNT + '" + str(intRegisterCount) + "',  "
                sqlModityStatistics += " APT_TRADE_REGISTER_MONEY = APT_TRADE_REGISTER_MONEY + '" + str(intRegisterMoney) + "'  "
                sqlModityStatistics += " WHERE unique_key = %s "
                nUpdateCount = nUpdateCount + 1

            else:
                #INSERT
                sqlModityStatistics  = " INSERT INTO " + strNewPartTable + " SET "
                sqlModityStatistics += " unique_key = %s, "
                sqlModityStatistics += " YYYY = '"+strBaseYYYY+"', "
                sqlModityStatistics += " YYYYMM = '" + strBaseYYYYMM + "', "
                sqlModityStatistics += " YYYYWEEK = '" + YYYYWEEK + "', "
                sqlModityStatistics += " ACC_YEAR = '" + strDBACC_YEAR + "' ,  "
                sqlModityStatistics += " SIDO_CD = '" + strDBSIDO_CD + "' ,  "
                sqlModityStatistics += " SIDO_NM = '" + strDBSIDO_NM + "' ,  "
                sqlModityStatistics += " SGG_CD = '" + strDBSGG_CD + "' ,  "
                sqlModityStatistics += " SGG_NM = '" + strDBSGG_NM + "' ,  "
                sqlModityStatistics += " BJDONG_CD = '" + strDBBJDONG_CD + "' ,  "
                sqlModityStatistics += " BJDONG_NM = '" + strDBBJDONG_NM + "' ,  "
                sqlModityStatistics += " HOUSE_TYPE = '" + strDBHOUSE_TYPE + "' ,  "
                sqlModityStatistics += " TOT_AREA = '" + strDBTOT_AREA + "' ,  "
                sqlModityStatistics += " APT_TRADE_CNT = '" + str(intTradeCount) + "', "
                sqlModityStatistics += " APT_TRADE_MONEY = '" + str(intTradeMoney) + "', "
                sqlModityStatistics += " APT_TRADE_CANCEL_CNT = '" + str(intCancelCount) + "', "
                sqlModityStatistics += " APT_TRADE_CANCEL_MONEY = '" + str(intCancelMoney) + "', "
                sqlModityStatistics += " APT_TRADE_REGISTER_CNT = '" + str(intRegisterCount) + "', "
                sqlModityStatistics += " APT_TRADE_REGISTER_MONEY = '" + str(intRegisterMoney) + "' "
                nInsertedCount = nInsertedCount + 1

            print(GetLogDef.lineno(__file__), "strDBTradeSequence > ", strDBTradeSequence)
            print(GetLogDef.lineno(__file__), "strAdminSection > ", strAdminSection)
            print(GetLogDef.lineno(__file__), "strAdminSection > ", strAdminSection)
            print(GetLogDef.lineno(__file__), "strDBDEAL_YMD > ", strDBDEAL_YMD)
            print(GetLogDef.lineno(__file__), "strUniqueKey > ", strUniqueKey)
            print(GetLogDef.lineno(__file__), "nInsertedCount ", nInsertedCount)
            print(GetLogDef.lineno(__file__), "nUpdateCount ", nUpdateCount)

            cursorRealEstate.execute(sqlModityStatistics, strUniqueKey)
            ResRealEstateConnection.commit()

            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = strDBTradeSequence
            dictSwitchData['data_2'] = strAdminSection
            dictSwitchData['data_3'] = strUniqueKey
            dictSwitchData['data_4'] = intMolitCount
            dictSwitchData['data_5'] = nUpdateCount
            dictSwitchData['data_6'] = nInsertedCount
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.lineno(__file__), "[END strAdminName]]================== ", strTradeSequence)


    except Exception as e:

        print(GetLogDef.lineno(__file__), "Error Exception")
        print(GetLogDef.lineno(__file__), e)
        print(GetLogDef.lineno(__file__), type(e))
        err_msg = traceback.format_exc()
        print(GetLogDef.lineno(__file__), err_msg)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        print(GetLogDef.lineno(__file__), 'Inserted => ', nInsertedCount, ' , Updated => ', nUpdateCount)
        print(GetLogDef.lineno(__file__), "========================= SUCCESS END")
    finally:
        print(GetLogDef.lineno(__file__), "Finally END")