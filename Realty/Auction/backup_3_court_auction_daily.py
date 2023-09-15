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

from Const import ConstRealEstateTable_AUC
from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from Init.DefConstant import ConstRealEstateTable
from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable

try:

    stToday = DateTime.today()

    nInsertedCount = 0
    nUpdateCount = 0

    #서울 부동산 실거래가 데이터 - 매매
    strProcessType = '021000'
    KuIndex = '00'
    arrCityPlace = '00'
    targetRow = '00'

    # print( nBaseStartDate, nBaseEndDate)
    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

    # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
    rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
    strResult = rstResult.get('result')
    if strResult is False:
        quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴

    if strResult == '10':
        quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴


    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '10'
    dictSwitchData['data_1'] = KuIndex
    dictSwitchData['data_2'] = arrCityPlace
    dictSwitchData['data_3'] = targetRow
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)



    # # Master Table 에 있는 테이블 전체 백업하기
    # qryBackupNaverMobileMaster = "INSERT INTO "+ConstRealEstateTable_AUC.CourtAuctionBackupTable+" SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable
    # cursorRealEstate.execute(qryBackupNaverMobileMaster)
    # nLoop = 0
    # print(str(cursorRealEstate.query))

    qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable+ " "
    cursorRealEstate.execute(qrySelectNaverMobileMaster)
    rstMasterDatas = cursorRealEstate.fetchall()

    nLoop = 0
    nMasterSeq = 0
    issue_number = '00'
    auction_code = '00'

    for MasterDataList in rstMasterDatas:
        nMasterSeq = str(MasterDataList.get('seq'))
        issue_number = str(MasterDataList.get('issue_number'))
        auction_code = str(MasterDataList.get('auction_code'))



        print(GetLogDef.lineno(__file__), "MasterDataList.seq => ", nMasterSeq)
        nLoop = nLoop + 1

        qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionBackupTable + " WHERE seq = %s"
        cursorRealEstate.execute(qrySelectNaverMobileMaster, nMasterSeq)
        rstBackupDatas = cursorRealEstate.fetchone()
        print(GetLogDef.lineno(__file__), "===================================> ", nMasterSeq, rstBackupDatas)

        if rstBackupDatas is None:
            qryInsertCourtAuctionBackup = " INSERT INTO " + ConstRealEstateTable_AUC.CourtAuctionBackupTable
            qryInsertCourtAuctionBackup += " (SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable
            qryInsertCourtAuctionBackup += " WHERE seq = %s)"
            print(GetLogDef.lineno(__file__), "INSERT =>", qryInsertCourtAuctionBackup, nMasterSeq)
            cursorRealEstate.execute(qryInsertCourtAuctionBackup, nMasterSeq)


        else:

            qryUpdateCourtAuctionBackup = "UPDATE " + ConstRealEstateTable_AUC.CourtAuctionBackupTable
            qryUpdateCourtAuctionBackup += " SET modify_date=NOW() "
            qryUpdateCourtAuctionBackup += " WHERE seq = %s "
            print(GetLogDef.lineno(__file__), "UPDATE =>", qryUpdateCourtAuctionBackup, nMasterSeq)
            cursorRealEstate.execute(qryUpdateCourtAuctionBackup, nMasterSeq)

        qryDeleteCourtAuctionData = " DELETE FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable
        qryDeleteCourtAuctionData += " WHERE seq = %s LIMIT 1"
        print(GetLogDef.lineno(__file__), "DELETE =>", qryDeleteCourtAuctionData, nMasterSeq)
        cursorRealEstate.execute(qryDeleteCourtAuctionData, nMasterSeq)

        # if(rstBackupDatas.get > 0):
        #     print(GetLogDef.lineno(__file__), "MasterDataList.seq => ", nMasterSeq)

    print("================For Loop", nLoop)
    # ResRealEstateConnection.rollback()
    ResRealEstateConnection.commit()


    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_3'] = nLoop
    dictSwitchData['data_4'] = nMasterSeq
    dictSwitchData['data_5'] = auction_code
    dictSwitchData['data_6'] = issue_number
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)



except Exception as e:

    ResRealEstateConnection.rollback()

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '30'
    dictSwitchData['data_3'] = nLoop
    dictSwitchData['data_4'] = nMasterSeq
    if strProcessType is not None:
        dictSwitchData['data_1'] = nMasterSeq

    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    print("Error Exception")
    print(e)
    print(type(e))

else:
    print("========================================================")


finally:
    ResRealEstateConnection.close()
    print("Finally END")

