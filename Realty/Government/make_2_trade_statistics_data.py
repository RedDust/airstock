# This is a sample Python script.
# 매일 09시 실행
import os
import sys
import json
import time
import random
import pymysql
import datetime
import traceback


sys.path.append("D:/PythonProjects/airstock")

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector

from Init.DefConstant import ConstRealEstateTable
from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.CustomException import QuitException




def main():

    try:
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        #네이버 부동산 일별 통계 데이터 작성
        strProcessType = '034110'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')

        if strResult is False:
            quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴

        if strResult == '10':
            #실행중이면 프로세서 중단 처리
            quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴

        if strResult == '20':
            #실행중이면 프로세서 중단 처리
            quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴

        if strResult == '30':
            #실행중이면 프로세서 중단 처리
            quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴


        nProcessedMasterSequence = rstResult.get('data_1')
        nMasterSeq = nMasterBaseSeq = nProcessedMasterSequence

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        #어제 데이터 부터 전체 처리
        qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.SeoulRealTradeDataTable + "  WHERE  seq > %s "
        # qrySelectNaverMobileMaster += " LIMIT 100 "
        cursorRealEstate.execute(qrySelectNaverMobileMaster, nMasterBaseSeq)
        rstMasterDatas = cursorRealEstate.fetchall()

        nLoop = 0
        for MasterDataList in rstMasterDatas:
            print(MasterDataList)

            nMasterSeq = str(MasterDataList.get('seq'))
            strHouseType = str(MasterDataList.get('HOUSE_TYPE'))

            dtDealYMD = str(MasterDataList.get('DEAL_YMD'))

            if strHouseType == '아파트':
                strRletTpCd = "A01"
            elif strHouseType == '연립다세대':
                strRletTpCd = "C02"
            elif strHouseType == '단독다가구':
                strRletTpCd = "C03"
            elif strHouseType == '오피스텔':
                strRletTpCd = "A02"
            else:
                print(GetLogDef.lineno(__file__), strHouseType)
                strRletTpCd = "ETC"

            strTradeType = '1'

            print(GetLogDef.lineno(__file__), dtDealYMD)
            # print(GetLogDef.lineno(__file__), dtDealYMD[0:4])
            # print(GetLogDef.lineno(__file__), dtDealYMD[4:6])
            # print(GetLogDef.lineno(__file__), dtDealYMD[6:8])


            dBaseIssueDatetime = datetime.date(int(dtDealYMD[0:4]), int(dtDealYMD[4:6]), int(dtDealYMD[6:8]))
            strBaseYYYY = str(dBaseIssueDatetime.year).zfill(4)
            strBaseMM = str(dBaseIssueDatetime.month).zfill(2)
            strBaseDD = str(dBaseIssueDatetime.day).zfill(2)
            strBaseYYYYMM = strBaseYYYY + strBaseMM
            strBaseYYYYMMDD = strBaseYYYYMM + strBaseDD

            strBaseYYYYMMDDWeekDay = dBaseIssueDatetime.weekday()

            YYYYWEEK = str(dBaseIssueDatetime.isocalendar().year) + str(dBaseIssueDatetime.isocalendar().week).zfill(2)
            YYYYWEEKDAY = dBaseIssueDatetime.isocalendar().weekday

            qrySelectStatisticsTable = " SELECT * FROM " + ConstRealEstateTable.SeoulRealTradeMasterStatisticsTable + " WHERE YYYYMMDD = %s "
            cursorRealEstate.execute(qrySelectStatisticsTable, strBaseYYYYMMDD)
            rstStatisticsDataList = cursorRealEstate.fetchone()
            # print(GetLogDef.lineno(__file__), rstStatisticsDataList)
            if rstStatisticsDataList is None:
                print("none insert")
                strFieldName = "count_"+strRletTpCd+"_"+strTradeType
                qryInsertStatisticsTable = "INSERT INTO " + ConstRealEstateTable.SeoulRealTradeMasterStatisticsTable + " SET YYYY = %s " \
                                                                                                                    ", YYYYMM = %s " \
                                                                                                                    ", YYYYMMDD = %s" \
                                                                                                                    ", YYYYWEEK = %s" \
                                                                                                                    ", YYYYWEEKDAY = %s" \
                                                                                                                    ", "+ strFieldName + " = 1 "
                cursorRealEstate.execute(qryInsertStatisticsTable,(strBaseYYYY, strBaseYYYYMM, strBaseYYYYMMDD,YYYYWEEK,YYYYWEEKDAY))
                print(qryInsertStatisticsTable, strBaseYYYY, strBaseYYYYMM, strBaseYYYYMMDD,YYYYWEEK,YYYYWEEKDAY)


            else:
                print("Update")
                strFieldName = "count_"+strRletTpCd+"_"+strTradeType
                qryInsertStatisticsTable = "UPDATE " + ConstRealEstateTable.SeoulRealTradeMasterStatisticsTable + " SET " + strFieldName + " = " + strFieldName +"+1 " \
                                                                                                                "WHERE YYYYMMDD = %s "
                cursorRealEstate.execute(qryInsertStatisticsTable,(strBaseYYYYMMDD))
                print(qryInsertStatisticsTable, strBaseYYYYMMDD)

            ResRealEstateConnection.commit()

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            nLoop = nLoop + 1
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = nMasterSeq
            dictSwitchData['data_2'] = dtDealYMD
            dictSwitchData['data_3'] = nLoop
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.lineno(__file__), "QuitException")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))
        return False

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        if strProcessType is not None:
            dictSwitchData['data_1'] = nMasterSeq

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        print("Error Exception")
        err_msg = traceback.format_exc()
        print(err_msg)

        print(e)
        print(type(e))
        return False
    else:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print("SUCCESS", "=====================================================================")
        return True
    finally:
        print("Finally END", "=====================================================================")
