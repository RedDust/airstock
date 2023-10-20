# This is a sample Python script.
# 매일 23시 실행
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

    date_1 = DateTime.today()
    end_date = date_1 - TimeDelta(days=2)
    nBaseStartDate = str(end_date.strftime('%Y-%m-%d 00:00:00'))
    nBaseEndDate = str(date_1.strftime('%Y-%m-%d 00:00:00'))

    nMasterSeq=0

    print( nBaseStartDate, nBaseEndDate)
    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

    # 정상처리
    # qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + "  WHERE  reg_date >= %s  AND reg_date < %s"
    # cursorRealEstate.execute(qrySelectNaverMobileMaster, (nBaseStartDate, nBaseEndDate))

    qrySelectSeoulSwitch = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterSwitchTable + " WHERE type='10'  "
    cursorRealEstate.execute(qrySelectSeoulSwitch)
    SwitchDataList = cursorRealEstate.fetchone()
    nMasterSeq = str(SwitchDataList.get('masterCortarNo'))


    #서울 부동산 실거래가 데이터 - 임대차
    strProcessType = '011000'
    KuIndex = '00'
    arrCityPlace = '00'
    targetRow = '00'

    # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
    rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
    strResult = rstResult.get('result')

    if strResult is False:
        quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴

    if strResult == '10':
        quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴


    nMasterSeq = str(rstResult.get('data_1'))



    #어제 데이터 부터 전체 처리
    qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + "  WHERE  seq >= %s "
    cursorRealEstate.execute(qrySelectNaverMobileMaster, nMasterSeq )
    rstMasterDatas = cursorRealEstate.fetchall()

    LibNaverMobileMasterSwitchTable.SwitchResultDataDistribution(nMasterSeq, "10")


    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '10'
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

    nLoop = 0
    for MasterDataList in rstMasterDatas:

        strTypeCode = str(MasterDataList.get('rletTpCd'))
        nMasterSeq = str(MasterDataList.get('seq'))
        strNewPartTable = ConstRealEstateTable.NaverMobileMasterTable + "_" + strTypeCode

        try:
            qryCheckTable = "SELECT count(*) FROM information_schema.tables WHERE TABLE_SCHEMA='"+ConstRealEstateTable.RealtyDatabase+"' AND TABLE_NAME='"+strNewPartTable+"'"
            cursorRealEstate.execute(qryCheckTable)
            results = cursorRealEstate.rowcount
            if results < 1:
                strNewPartTable = ConstRealEstateTable.NaverMobileMasterTable + "_ETC"


            qrySelectBackupTable = "SELECT * FROM " + strNewPartTable + " WHERE seq=%s"
            cursorRealEstate.execute(qrySelectBackupTable, nMasterSeq)

            nResultCount = cursorRealEstate.rowcount
            if nResultCount < 1:
                #Insert
                qryInsertTypeTable = "INSERT INTO "+strNewPartTable+" SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + " WHERE seq = %s"
                cursorRealEstate.execute(qryInsertTypeTable, nMasterSeq)



            # nMasterSeq = cursorRealEstate.lastrowid
            ResRealEstateConnection.commit()

        except Exception as e:
            print(e)
        #try-except End

    #for MasterDataList in rstMasterDatas END

    ResRealEstateConnection.close()

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_1'] = nMasterSeq
    dictSwitchData['data_2'] = strTypeCode
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


