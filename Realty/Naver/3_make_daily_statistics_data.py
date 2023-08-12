# This is a sample Python script.
# 매일 09시 실행
import os
import sys
import json
import time
import random
import pymysql
import datetime
sys.path.append("D:/PythonProjects/airstock")

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector

from Init.DefConstant import ConstRealEstateTable
from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable


try:





    # print( nBaseStartDate, nBaseEndDate)
    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

    #네이버 부동산 일별 통계 데이터 작성
    strProcessType = '012000'
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


    nProcessedMasterSequence = rstResult.get('data_1')
    nMasterBaseSeq = nProcessedMasterSequence
    nMasterSeq = nProcessedMasterSequence

    #어제 데이터 부터 전체 처리
    qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + "  WHERE  seq >= %s "
    cursorRealEstate.execute(qrySelectNaverMobileMaster, nMasterBaseSeq)
    rstMasterDatas = cursorRealEstate.fetchall()


    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '10'
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

    # print(qrySelectNaverMobileMaster, type(qrySelectNaverMobileMaster))

    nLoop = 0
    for MasterDataList in rstMasterDatas:


        # print(MasterDataList)

        nMasterSeq = str(MasterDataList.get('seq'))


        strRletTpCd = str(MasterDataList.get('rletTpCd'))
        strRletTpNm = str(MasterDataList.get('rletTpNm'))

        strTradTpCd = str(MasterDataList.get('tradTpCd'))
        strTradTpNm = str(MasterDataList.get('tradTpNm'))

        strAtclCfmYmd = str(MasterDataList.get('atclCfmYmd'))
        arrAtclCfmYmd = strAtclCfmYmd.split(".")

        # print(strAtclCfmYmd,  ":", strRletTpCd, strRletTpNm, "/", strTradTpCd, strTradTpNm)
        # print(arrAtclCfmYmd[0], arrAtclCfmYmd[1], arrAtclCfmYmd[2])

        dBaseIssueDatetime = datetime.date(int("20"+arrAtclCfmYmd[0]), int(arrAtclCfmYmd[1]), int(arrAtclCfmYmd[2]))
        strBaseYYYY = str(dBaseIssueDatetime.year).zfill(4)
        strBaseMM = str(dBaseIssueDatetime.month).zfill(2)
        strBaseDD = str(dBaseIssueDatetime.day).zfill(2)
        strBaseYYYYMM = strBaseYYYY + strBaseMM
        strBaseYYYYMMDD = strBaseYYYYMM + strBaseDD

        strBaseYYYYMMDDWeekDay = dBaseIssueDatetime.weekday()

        YYYYWEEK = str(dBaseIssueDatetime.isocalendar().year) + str(dBaseIssueDatetime.isocalendar().week)
        YYYYWEEKDAY = dBaseIssueDatetime.isocalendar().weekday

        qrySelectStatisticsTable = " SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterStatisticsTable + " WHERE YYYYMMDD = %s "
        cursorRealEstate.execute(qrySelectStatisticsTable, strBaseYYYYMMDD)

        rstStatisticsDataList = cursorRealEstate.fetchone()


        if strTradTpCd == 'A1':
            strTradeType = '1'
        elif strTradTpCd == 'B1':
            strTradeType = '2'
        elif strTradTpCd == 'B2':
            strTradeType = '3'
        else:
            strTradeType = '4'


        if rstStatisticsDataList is None:
            print("none insert")
            strFieldName = "count_"+strRletTpCd+"_"+strTradeType
            qryInsertStatisticsTable = "INSERT INTO " + ConstRealEstateTable.NaverMobileMasterStatisticsTable + " SET YYYY = %s " \
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
            qryInsertStatisticsTable = "UPDATE " + ConstRealEstateTable.NaverMobileMasterStatisticsTable + " SET " + strFieldName + " = " + strFieldName +"+1 " \
                                                                                                            "WHERE YYYYMMDD = %s "
            cursorRealEstate.execute(qryInsertStatisticsTable,(strBaseYYYYMMDD))
            print(qryInsertStatisticsTable, strBaseYYYYMMDD)


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        nLoop = nLoop + 1
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = nMasterSeq
        dictSwitchData['data_2'] = strAtclCfmYmd
        dictSwitchData['data_3'] = nLoop
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        ResRealEstateConnection.commit()


    ResRealEstateConnection.close()

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_1'] = nMasterSeq
    dictSwitchData['data_2'] = strAtclCfmYmd
    dictSwitchData['data_3'] = nLoop
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

except Exception as e:

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '30'
    if strProcessType is not None:
        dictSwitchData['data_1'] = nMasterSeq

    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    print("Error Exception")
    print(e)
    print(type(e))

else:
    print("========================================================")
finally:
    print("Finally END")


