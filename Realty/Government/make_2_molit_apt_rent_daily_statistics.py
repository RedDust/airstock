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

        #국토교통부 아파트 매매 임대차 통계 작성 - 임대차
        strProcessType = '035212'
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

        sqlSelectMOLIT = " SELECT * FROM " + ConstRealEstateTable_GOV.MolitRealRentMasterTable + " "
        sqlSelectMOLIT += " WHERE seq > %s "
        sqlSelectMOLIT += " ORDER BY seq ASC "
        # sqlSelectMOLIT += " LIMIT 1 "

        cursorRealEstate.execute(sqlSelectMOLIT , strTradeSequence )
        intMolitCount = cursorRealEstate.rowcount
        print(GetLogDef.lineno(__file__), "intMolitCount", intMolitCount)

        rstSelectDatas = cursorRealEstate.fetchall()

        for rstSelectData in rstSelectDatas:

            print("============================================================================================================")

            print("rstSelectData >> " , rstSelectData)

            strDBTradeSequence = str(rstSelectData.get('seq'))

            strDBDEAL_YMD = str(rstSelectData.get('DEAL_YMD'))
            strDBSIDO_CD = str(rstSelectData.get('SIDO_CD'))
            strDBSIDO_NM = str(rstSelectData.get('SIDO_NM'))
            strDBSGG_CD = str(rstSelectData.get('SGG_CD'))
            strDBSGG_NM = str(rstSelectData.get('SGG_NM'))
            strDBBJDONG_CD = str(rstSelectData.get('BJDONG_CD'))
            strDBBJDONG_NM = str(rstSelectData.get('BJDONG_NM'))
            strDBHOUSE_TYPE = str(rstSelectData.get('HOUSE_TYPE'))
            listDBTOT_AREA = str(rstSelectData.get('TOT_AREA')).split('.')
            intDBRENT_AMT = int(rstSelectData.get('RENT_AMT'))
            intDBDEPOSIT_AMT= int(rstSelectData.get('DEPOSIT_AMT'))
            print("listDBTOT_AREA >> " , type(listDBTOT_AREA), listDBTOT_AREA)
            strDBTOT_AREA = listDBTOT_AREA[0]
            print("1strDBTOT_AREA >> " , type(strDBTOT_AREA), strDBTOT_AREA)
            if len(strDBTOT_AREA) < 1:
                strDBTOT_AREA =str(rstSelectData.get('TOT_AREA'))

            if len(strDBTOT_AREA) < 1:
                strDBTOT_AREA='0'

            print("2strDBTOT_AREA >> " , type(strDBTOT_AREA), strDBTOT_AREA)
            intDBTOT_AREA = int(strDBTOT_AREA)


            strAdminSection = strDBSIDO_NM + " " + strDBSGG_NM + " " + strDBBJDONG_NM
            strBaseYYYY = (strDBDEAL_YMD[0:4])
            strBaseMM = (strDBDEAL_YMD[4:6])
            strBaseDD = (strDBDEAL_YMD[6:8])
            strBaseYYYYMM = strBaseYYYY + strBaseMM
            strBaseYYYYMMDD = strBaseYYYYMM + strBaseDD
            strDBACC_YEAR = strBaseYYYY

            dBaseIssueDatetime = datetime.date(int(strBaseYYYY), int(strBaseMM), int(strBaseDD))

            strBaseYYYYMMDDWeekDay = dBaseIssueDatetime.weekday()

            YYYYWEEK = str(dBaseIssueDatetime.isocalendar().year) + str(dBaseIssueDatetime.isocalendar().week).zfill(2)
            YYYYWEEKDAY = str(dBaseIssueDatetime.isocalendar().weekday)

            strUniqueKey = YYYYWEEK + "_" + strDBSIDO_CD + strDBSGG_CD + strDBBJDONG_CD
            strUniqueKey += "_" + strDBHOUSE_TYPE + "_" + strDBTOT_AREA

            strNewPartTable = ConstRealEstateTable_GOV.MolitRealTradeBasicAPTRentStatisticsMasterTable + "_" + strDBSIDO_CD

            qryCheckTable = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA='"+ConstRealEstateTable_GOV.RealtyDatabase+"' AND TABLE_NAME='"+strNewPartTable+"'"
            cursorRealEstate.execute(qryCheckTable)

            print(GetLogDef.lineno(__file__), "qryCheckTable > ", qryCheckTable)

            results = cursorRealEstate.rowcount

            print(GetLogDef.lineno(__file__), "results > ", results)

            if results < 1:
                qryCreateBackupTable = "create table " + strNewPartTable + " LIKE " + ConstRealEstateTable_GOV.MolitRealTradeBasicAPTRentStatisticsMasterTable
                print(GetLogDef.lineno(__file__), "qryCreateBackupTable > ", qryCreateBackupTable)
                cursorRealEstate.execute(qryCreateBackupTable)

            intAPT_FULL_DEPOSIT_MONEY=0
            intAPT_FULL_RENT_CNT = 0
            intAPT_FULL_RENT_UNIT_PRICE = 0

            intAPT_MONTLY_RENT_CNT = 0
            intAPT_MONTLY_RENT_DEPOSIT_MONEY = 0
            intAPT_MONTLY_RENT_MONEY = 0
            intAPT_MONTLY_RENT_UNIT_PRICE = 0
            intAPT_MONTLY_RENT_CONVERSION_PRICE = 0
            intAPT_MONTLY_RENT_CONVERSION_RATIO = 4

            if intDBRENT_AMT < 1 :
            # 전세일 경우
                intAPT_FULL_DEPOSIT_MONEY = intDBDEPOSIT_AMT
                intAPT_FULL_RENT_CNT = 1
                intAPT_FULL_RENT_UNIT_PRICE = 0

            else:
            #월세인 경우
                intAPT_MONTLY_RENT_CNT = 1
                intAPT_MONTLY_RENT_DEPOSIT_MONEY = intDBDEPOSIT_AMT
                intAPT_MONTLY_RENT_MONEY = intDBRENT_AMT
                intAPT_MONTLY_RENT_CONVERSION_PRICE = intDBDEPOSIT_AMT + (intDBRENT_AMT * 250)
                intAPT_MONTLY_RENT_UNIT_PRICE = 0

            print(GetLogDef.lineno(__file__), "strUniqueKey > ", strUniqueKey)

            sqlSelectStatistics = " SELECT * FROM " + strNewPartTable + " "
            sqlSelectStatistics += " WHERE unique_key = %s "

            cursorRealEstate.execute(sqlSelectStatistics, strUniqueKey)
            intMolitStaticCount = cursorRealEstate.rowcount


            if intMolitStaticCount > 0:

                rstStaticsData = cursorRealEstate.fetchone()
                print(GetLogDef.lineno(__file__), "rstStaticsData > ", rstStaticsData)
                intStatisticsDBAPT_FULL_RENT_CNT = int(rstStaticsData.get('APT_FULL_RENT_CNT'))
                intStatisticsDBAPT_FULL_DEPOSIT_MONEY = int(rstStaticsData.get('APT_FULL_DEPOSIT_MONEY'))
                intStatisticsDBAPT_FULL_RENT_UNIT_PRICE = int(rstStaticsData.get('APT_FULL_RENT_UNIT_PRICE'))
                intStatisticsDBAPT_FULL_RENT_CANCEL_CNT = int(rstStaticsData.get('APT_FULL_RENT_CANCEL_CNT'))
                intStatisticsDBAPT_FULL_RENT_CANCEL_MONEY = int(rstStaticsData.get('APT_FULL_RENT_CANCEL_MONEY'))

                intStatisticsDBAPT_MONTLY_RENT_CNT = int(rstStaticsData.get('APT_MONTLY_RENT_CNT'))
                intStatisticsDBAPT_MONTLY_RENT_DEPOSIT_MONEY = int(rstStaticsData.get('APT_MONTLY_RENT_DEPOSIT_MONEY'))
                intStatisticsDBAPT_MONTLY_RENT_MONEY = int(rstStaticsData.get('APT_MONTLY_RENT_MONEY'))
                intStatisticsDBAPT_MONTLY_RENT_UNIT_PRICE = int(rstStaticsData.get('APT_MONTLY_RENT_UNIT_PRICE'))
                intStatisticsDBAPT_MONTLY_RENT_CONVERSION_PRICE = int(rstStaticsData.get('APT_MONTLY_RENT_CONVERSION_PRICE'))
                intStatisticsDBAPT_MONTLY_RENT_CONVERSION_RATIO = int(rstStaticsData.get('APT_MONTLY_RENT_CONVERSION_RATIO'))
                intStatisticsDBAPT_MONTLY_RENT_CANCEL_CNT = int(rstStaticsData.get('APT_MONTLY_RENT_CANCEL_CNT'))
                intStatisticsDBAPT_MONTLY_RENT_CANCEL_MONEY = int(rstStaticsData.get('APT_MONTLY_RENT_CANCEL_MONEY'))
                intStatisticsDBTOT_AREA = int(rstStaticsData.get('TOT_AREA'))

                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_FULL_RENT_CNT > ", intStatisticsDBAPT_FULL_RENT_CNT)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_FULL_DEPOSIT_MONEY > ", intStatisticsDBAPT_FULL_DEPOSIT_MONEY)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_FULL_RENT_UNIT_PRICE > ", intStatisticsDBAPT_FULL_RENT_UNIT_PRICE)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_FULL_RENT_CANCEL_CNT > ", intStatisticsDBAPT_FULL_RENT_CANCEL_CNT)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_FULL_RENT_CANCEL_MONEY > ", intStatisticsDBAPT_FULL_RENT_CANCEL_MONEY)
                #
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_MONTLY_RENT_CNT > ", intStatisticsDBAPT_MONTLY_RENT_CNT)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_MONTLY_RENT_DEPOSIT_MONEY > ", intStatisticsDBAPT_MONTLY_RENT_DEPOSIT_MONEY)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_MONTLY_RENT_MONEY > ", intStatisticsDBAPT_MONTLY_RENT_MONEY)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_MONTLY_RENT_UNIT_PRICE > ", intStatisticsDBAPT_MONTLY_RENT_UNIT_PRICE)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_MONTLY_RENT_CONVERSION_PRICE > ", intStatisticsDBAPT_MONTLY_RENT_CONVERSION_PRICE)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_MONTLY_RENT_CONVERSION_RATIO > ", intStatisticsDBAPT_MONTLY_RENT_CONVERSION_RATIO)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_MONTLY_RENT_CANCEL_CNT > ", intStatisticsDBAPT_MONTLY_RENT_CANCEL_CNT)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_MONTLY_RENT_CANCEL_MONEY > ", intStatisticsDBAPT_MONTLY_RENT_CANCEL_MONEY)
                # print(GetLogDef.lineno(__file__), "intStatisticsDBTOT_AREA > ", intStatisticsDBTOT_AREA)


                #전세 건수 합
                intAPT_FULL_RENT_CNT = intAPT_FULL_RENT_CNT + intStatisticsDBAPT_FULL_RENT_CNT
                print(GetLogDef.lineno(__file__), "intAPT_FULL_RENT_CNT > ", intAPT_FULL_RENT_CNT)

                # 전세 보증금 전체 합
                intAPT_FULL_DEPOSIT_MONEY = intAPT_FULL_DEPOSIT_MONEY + intStatisticsDBAPT_FULL_DEPOSIT_MONEY
                print(GetLogDef.lineno(__file__), "intAPT_FULL_DEPOSIT_MONEY > ", intAPT_FULL_DEPOSIT_MONEY)

                # 전세 보증금 평단가
                if intAPT_FULL_DEPOSIT_MONEY > 1 and intAPT_FULL_RENT_CNT > 0 and intDBTOT_AREA > 0:
                    intAPT_FULL_RENT_UNIT_PRICE = round(intAPT_FULL_DEPOSIT_MONEY / intAPT_FULL_RENT_CNT / intDBTOT_AREA)

                intAPT_FULL_RENT_UNIT_PRICE += intStatisticsDBAPT_FULL_RENT_UNIT_PRICE

                print(GetLogDef.lineno(__file__), "intAPT_FULL_RENT_UNIT_PRICE > ", intAPT_FULL_RENT_UNIT_PRICE)



                #월세 건수 합
                intAPT_MONTLY_RENT_CNT = intAPT_MONTLY_RENT_CNT + intStatisticsDBAPT_MONTLY_RENT_CNT
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_RENT_CNT > ", intAPT_MONTLY_RENT_CNT)


                # 월세 보증금 전체 합 (해당 월세 보증금 + DB보증금)
                intAPT_MONTLY_DEPOSIT_SUM = (intAPT_MONTLY_RENT_DEPOSIT_MONEY + intStatisticsDBAPT_FULL_DEPOSIT_MONEY )
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_DEPOSIT_SUM > ",intAPT_MONTLY_DEPOSIT_SUM)

                # 월세 임대료 전체 합  (해당 월세 + DB 월세)
                intAPT_MONTLY_DEPOSIT_RENT_SUM = (intAPT_MONTLY_RENT_MONEY + intStatisticsDBAPT_MONTLY_RENT_MONEY)
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_DEPOSIT_RENT_SUM > ", intAPT_MONTLY_DEPOSIT_RENT_SUM)

                # 월세 환산보증금  (해당 월세의 -> 환산보증금)
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_RENT_CONVERSION_RATIO > ", intAPT_MONTLY_RENT_CONVERSION_RATIO)
                intAPT_MONTLY_RENT_CONVERSION = intAPT_MONTLY_DEPOSIT_RENT_SUM * round((1000 / intAPT_MONTLY_RENT_CONVERSION_RATIO))
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_DEPOSIT_RENT_SUM > ", intAPT_MONTLY_DEPOSIT_RENT_SUM)

                # 월세 보증금 + 환산보증금
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_RENT_CONVERSION > ",type(intAPT_MONTLY_RENT_CONVERSION), intAPT_MONTLY_RENT_CONVERSION)
                print(GetLogDef.lineno(__file__), "intStatisticsDBAPT_MONTLY_RENT_CONVERSION_PRICE > ", type(intStatisticsDBAPT_MONTLY_RENT_CONVERSION_PRICE),intStatisticsDBAPT_MONTLY_RENT_CONVERSION_PRICE)
                intAPT_MONTLY_RENT_CONVERSION_PRICE = (intAPT_MONTLY_RENT_CONVERSION + intStatisticsDBAPT_MONTLY_RENT_CONVERSION_PRICE)

                # 환산 보증금으로 평단가
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_RENT_CONVERSION_PRICE > ",type(intAPT_MONTLY_RENT_CONVERSION_PRICE), intAPT_MONTLY_RENT_CONVERSION_PRICE)
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_RENT_CNT > ", type(intAPT_MONTLY_RENT_CNT),intAPT_MONTLY_RENT_CNT)
                print(GetLogDef.lineno(__file__), "intDBTOT_AREA > ", type(intDBTOT_AREA), intDBTOT_AREA)


                if intAPT_MONTLY_RENT_CONVERSION_PRICE > 0 and intAPT_MONTLY_RENT_CNT > 0 and intDBTOT_AREA>0:
                    intAPT_MONTLY_RENT_UNIT_PRICE = round(intAPT_MONTLY_RENT_CONVERSION_PRICE / intAPT_MONTLY_RENT_CNT / intDBTOT_AREA )

                intAPT_MONTLY_RENT_UNIT_PRICE += intStatisticsDBAPT_MONTLY_RENT_UNIT_PRICE

                sqlModifyStatistics  = " UPDATE " + strNewPartTable + " SET "
                sqlModifyStatistics += " APT_FULL_RENT_CNT = '" + str(intAPT_FULL_RENT_CNT) + "'  ,  "
                sqlModifyStatistics += " APT_FULL_DEPOSIT_MONEY = '" + str(intAPT_FULL_DEPOSIT_MONEY) + "'  ,  "
                sqlModifyStatistics += " APT_FULL_RENT_UNIT_PRICE = '" + str(intAPT_FULL_RENT_UNIT_PRICE) + "'  ,  "
                sqlModifyStatistics += " APT_MONTLY_RENT_CNT = '" + str(intAPT_MONTLY_RENT_CNT) + "' ,  "
                sqlModifyStatistics += " APT_MONTLY_RENT_DEPOSIT_MONEY = '" + str(intAPT_MONTLY_DEPOSIT_SUM) + "' ,  "
                sqlModifyStatistics += " APT_MONTLY_RENT_MONEY = '" + str(intAPT_MONTLY_DEPOSIT_RENT_SUM) + "',  "
                sqlModifyStatistics += " APT_MONTLY_RENT_CONVERSION_PRICE = '" + str(intAPT_MONTLY_RENT_CONVERSION_PRICE) + "', "
                sqlModifyStatistics += " APT_MONTLY_RENT_CONVERSION_RATIO = '" + str(intAPT_MONTLY_RENT_CONVERSION_RATIO) + "',  "
                sqlModifyStatistics += " APT_MONTLY_RENT_UNIT_PRICE = '" + str(intAPT_MONTLY_RENT_UNIT_PRICE) + "' "
                sqlModifyStatistics += " WHERE unique_key = %s "
                nUpdateCount = nUpdateCount + 1

            else:

                print(GetLogDef.lineno(__file__), "intAPT_FULL_DEPOSIT_MONEY > ", type(intAPT_FULL_DEPOSIT_MONEY) , intAPT_FULL_DEPOSIT_MONEY)
                print(GetLogDef.lineno(__file__), "intAPT_FULL_RENT_CNT > ", type(intAPT_FULL_RENT_CNT) ,  intAPT_FULL_RENT_CNT)
                print(GetLogDef.lineno(__file__), "strDBTOT_AREA > ", type(strDBTOT_AREA) ,  strDBTOT_AREA)
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_RENT_CONVERSION_PRICE > ", type(intAPT_MONTLY_RENT_CONVERSION_PRICE) ,  intAPT_MONTLY_RENT_CONVERSION_PRICE)
                print(GetLogDef.lineno(__file__), "intAPT_MONTLY_RENT_CNT > ", type(intAPT_MONTLY_RENT_CNT) ,  intAPT_MONTLY_RENT_CNT)

                intAPT_FULL_RENT_UNIT_PRICE = 0
                intAPT_MONTLY_RENT_UNIT_PRICE = 0


                if intAPT_FULL_DEPOSIT_MONEY > 1 and intDBTOT_AREA>0:
                    intAPT_FULL_RENT_UNIT_PRICE = round(intAPT_FULL_DEPOSIT_MONEY / intAPT_FULL_RENT_CNT / intDBTOT_AREA)

                if intAPT_MONTLY_RENT_CONVERSION_PRICE > 1 and intDBTOT_AREA>0:
                    intAPT_MONTLY_RENT_UNIT_PRICE = round(intAPT_MONTLY_RENT_CONVERSION_PRICE / intAPT_MONTLY_RENT_CNT / intDBTOT_AREA)

                # Insert
                sqlModifyStatistics = " INSERT INTO " + strNewPartTable + " SET "
                sqlModifyStatistics += " unique_key = %s, "
                sqlModifyStatistics += " YYYY = '"+strBaseYYYY+"', "
                sqlModifyStatistics += " YYYYMM = '" + strBaseYYYYMM + "', "
                sqlModifyStatistics += " YYYYWEEK = '" + YYYYWEEK + "', "
                sqlModifyStatistics += " ACC_YEAR = '" + strDBACC_YEAR + "' ,  "
                sqlModifyStatistics += " SIDO_CD = '" + strDBSIDO_CD + "' ,  "
                sqlModifyStatistics += " SIDO_NM = '" + strDBSIDO_NM + "' ,  "
                sqlModifyStatistics += " SGG_CD = '" + strDBSGG_CD + "' ,  "
                sqlModifyStatistics += " SGG_NM = '" + strDBSGG_NM + "' ,  "
                sqlModifyStatistics += " BJDONG_CD = '" + strDBBJDONG_CD + "' ,  "
                sqlModifyStatistics += " BJDONG_NM = '" + strDBBJDONG_NM + "' ,  "
                sqlModifyStatistics += " HOUSE_TYPE = '" + strDBHOUSE_TYPE + "' ,  "
                sqlModifyStatistics += " TOT_AREA = '" + strDBTOT_AREA + "' ,  "
                sqlModifyStatistics += " APT_FULL_RENT_CNT = '" + str(intAPT_FULL_RENT_CNT) + "', "
                sqlModifyStatistics += " APT_FULL_DEPOSIT_MONEY = '" + str(intAPT_FULL_DEPOSIT_MONEY) + "', "
                sqlModifyStatistics += " APT_FULL_RENT_UNIT_PRICE = '" + str(intAPT_FULL_RENT_UNIT_PRICE) + "', "
                sqlModifyStatistics += " APT_MONTLY_RENT_DEPOSIT_MONEY = '" + str(intAPT_MONTLY_RENT_DEPOSIT_MONEY) + "', "
                sqlModifyStatistics += " APT_MONTLY_RENT_MONEY = '" + str(intAPT_MONTLY_RENT_MONEY) + "', "
                sqlModifyStatistics += " APT_MONTLY_RENT_CNT = '" + str(intAPT_MONTLY_RENT_CNT) + "', "
                sqlModifyStatistics += " APT_MONTLY_RENT_CONVERSION_RATIO = '" + str(intAPT_MONTLY_RENT_CONVERSION_RATIO) + "', "
                sqlModifyStatistics += " APT_MONTLY_RENT_CONVERSION_PRICE = '" + str(intAPT_MONTLY_RENT_CONVERSION_PRICE) + "', "
                sqlModifyStatistics += " APT_MONTLY_RENT_UNIT_PRICE = '" + str(intAPT_MONTLY_RENT_UNIT_PRICE) + "' "

                nInsertedCount = nInsertedCount + 1

            print(GetLogDef.lineno(__file__), "strDBTradeSequence > ", strDBTradeSequence)
            print(GetLogDef.lineno(__file__), "sqlModifyStatistics > ", sqlModifyStatistics)
            print(GetLogDef.lineno(__file__), "strUniqueKey > ", strUniqueKey)
            # print(GetLogDef.lineno(__file__), "strAdminSection > ", strAdminSection)
            # print(GetLogDef.lineno(__file__), "strDBDEAL_YMD > ", strDBDEAL_YMD)
            # print(GetLogDef.lineno(__file__), "nInsertedCount ", nInsertedCount)
            # print(GetLogDef.lineno(__file__), "nUpdateCount ", nUpdateCount)

            cursorRealEstate.execute(sqlModifyStatistics, strUniqueKey)
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



if __name__ == '__main__':
    main()