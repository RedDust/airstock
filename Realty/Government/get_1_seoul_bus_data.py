# 서울시 버스 사용량
#
# 2023-05-02 커밋
#https://data.seoul.go.kr/dataList/OA-12912/S/1/datasetView.do
# get_1_seoul_bus_data_with_multi.py 파일로 변경
# 2023-09-30 현재 사용 하지 않음

quit(500)

import datetime
import sys
sys.path.append("D:/PythonProjects/airstock")


import urllib.request
import json
import pymysql
import traceback
import math

import pandas as pd

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from Lib.RDB import pyMysqlConnector
from Lib.CustomException import QuitException
from Helper import hash_fnc

from SeoulLib.RDB import LibSeoulRealTradeSwitch

from Init.Functions.Logs import GetLogDef

from datetime import datetime as DateTime, timedelta as TimeDelta

from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable

# 한번에 처리할 건수
nProcessedCount = 1000

nTotalCount = 0

# 실제[]
nLoopTotalCount = 0



try:

    #서울 부동산 실거래가 데이터 - 서울 버스 사용량
    strProcessType = '034170'
    KuIndex = '00'
    arrCityPlace = '00'
    targetRow = '00'

    QuitException.QuitException(GetLogDef.lineno(__file__))  # 예외를 발생시킴


    # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
    rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
    strResult = rstResult.get('result')
    if strResult is False:
        QuitException.QuitException(GetLogDef.lineno(__file__))  # 예외를 발생시킴

    if strResult == '10':
        QuitException.QuitException(GetLogDef.lineno(__file__))  # 예외를 발생시킴

    if strResult == '30':
        QuitException.QuitException(GetLogDef.lineno(__file__))  # 예외를 발생시킴




    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '10'
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

    #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
    #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
    #거래 신고 30일 + 취소 신고 +30일
    dtToday = DateTime.today()
    # nBaseProcessDate = 50004

    # nBaseProcessDate = 3

    nBaseProcessDate = 0

    nEmptyCount = 0

    nCount = 0



    nFinalDate = 20211231

    #nFinalDate = 20230910

    print(GetLogDef.lineno(), "dtToday >", dtToday, type(dtToday))

    # nBaseDate = 20221231

    dtToday = datetime.datetime(2022, 12, 31, 00, 00, 00)
    nStartNumber = 1
    nEndNumber = 1000

    print(GetLogDef.lineno(), "dtToday >", dtToday, type(dtToday))

    # quit("555555555")

    while True:


        end_date = dtToday - TimeDelta(days=nBaseProcessDate)
        nBaseDate = int(end_date.strftime('%Y%m%d'))

        if nFinalDate >= nBaseDate:
            print(GetLogDef.lineno(__file__), "finalDate > ", nFinalDate, nBaseDate)
            break

        # 시작번호
        nStartNumber = 1
        # 최종번호
        nEndNumber = nProcessedCount

        #검색기준일을 미리 1 올려 놓는다.
        nBaseProcessDate += 1


#SeoulBusDataTable ====================================================
# D:\PythonProjects\airstock\Init\Functions\Logs\GetLogDef.py(111) nBaseDate > 20230622 <class 'int'>
# D:\PythonProjects\airstock\Init\Functions\Logs\GetLogDef.py(112) nStartNumber > 20001 <class 'int'>
# D:\PythonProjects\airstock\Init\Functions\Logs\GetLogDef.py(113) nEndNumber > 21000 <class 'int'>
# D:\PythonProjects\airstock\Realty\Government\get_1_seoul_bus_data.py(124) url >  http://openapi.seoul.go.kr:8088/644b72616d7265643132376d67576a6c/json/CardBusStatisticsServiceNew/20001/21000/20230622
# D:\PythonProjects\airstock\Realty\Government\get_1_seoul_bus_data.py(134) json_object.get('RESULT')  >  None
# nTotalCount >  40673


        while True:


            print(GetLogDef.lineno(), "nBaseDate >", nBaseDate, type(nBaseDate))
            print(GetLogDef.lineno(), "nStartNumber >", nStartNumber, type(nStartNumber))
            print(GetLogDef.lineno(), "nEndNumber >", nEndNumber, type(nEndNumber))

            nCount += 1
            # if nCount > 3:
            #     print(GetLogDef.lineno(__file__), "nCount > ", nCount)
            #     break


            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮

            url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/CardBusStatisticsServiceNew/"+str(nStartNumber)+"/"+str(nEndNumber)+"/" +str(nBaseDate)
            print(GetLogDef.lineno(__file__), "url > ", url)


            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            response = urllib.request.urlopen(url)
            json_str = response.read().decode("utf-8")

            # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
            json_object = json.loads(json_str)

            print(GetLogDef.lineno(__file__), "json_object.get('RESULT')  > ", json_object.get('RESULT') )

            #조회된 데이터가 "있으면" json_object.get('RESULT') => None
            if json_object.get('RESULT') != None:
                print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('CODE'))
                print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('MESSAGE'))
                break

            bMore = json_object.get('CardBusStatisticsServiceNew')
            if bMore is None:
                Exception(GetLogDef.lineno(__file__), 'bMore => ', bMore)  # 예외를 발생시킴
                break

            nTotalCount = bMore.get('list_total_count')
            print("nTotalCount > ", nTotalCount)


            jsonResultDatas = bMore.get('RESULT')
            jsonResultDatasResult = jsonResultDatas

            strResultCode = jsonResultDatasResult.get('CODE')
            strResultMessage = jsonResultDatasResult.get('MESSAGE')

            if strResultCode != 'INFO-000':
                Exception(GetLogDef.lineno(), 'strResultCode => ', strResultCode , strResultMessage)  # 예외를 발생시킴
                break
                # GetOut while True:

            jsonRowDatas = bMore.get('row')


            # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

            dictSeoulColumnInfoData = {}

            #필드 테이블 컬럼 타입 조회 int 인경우는 예외처리 하기 위한 준비
            qrySelectSeoulTableColumns = "show columns from " + ConstRealEstateTable_GOV.SeoulBusDataTable
            cursorRealEstate.execute(qrySelectSeoulTableColumns)
            SelectColumnLists = cursorRealEstate.fetchall()

            for SelectColumnList in SelectColumnLists:
                strFieldName = str(SelectColumnList.get('Field'))
                strColumnType = str(SelectColumnList.get('Type'))
                dictSeoulColumnInfoData[strFieldName] = {}
                dictSeoulColumnInfoData[strFieldName]['name'] = strFieldName
                dictSeoulColumnInfoData[strFieldName]['type'] = strColumnType

                print("SelectColumnList > ", SelectColumnList)

            print("dictSeoulColumnInfoData >" , dictSeoulColumnInfoData)

            strResultCode = jsonResultDatasResult.get('CODE')
            strResultMessage = jsonResultDatasResult.get('MESSAGE')


            print("Processing", "====================================================")
            nInsertedCount = 0
            nUpdateCount = 0
            nLoop = 0
            insertDict = dict()

            for list in jsonRowDatas:
                # print("[ "+str(nStartNumber)+" - "+str(nEndNumber)+" ][ "+str(nLoop)+" ] ")
                nLoop += 1

                dictSeoulRealtyTradeDataMaster = {}

                for dictSeoulMasterKeys in list.keys():

                    for dictNaverMobileMasterKeys in list.keys():
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = list.get(dictSeoulMasterKeys)

                    # Non-strings are converted to strings.
                    if type(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys]) is not str:
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = str( dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys])

                # for dictSeoulMasterKeys in list.keys():

                strUniqueKey = dictSeoulRealtyTradeDataMaster['USE_DT'] + "_"
                strUniqueKey += dictSeoulRealtyTradeDataMaster['BUS_ROUTE_ID'] + "_"
                strUniqueKey += dictSeoulRealtyTradeDataMaster['STND_BSST_ID'] + "_"
                strUniqueKey += hash_fnc.MD5EncodeFunction(dictSeoulRealtyTradeDataMaster['BUS_STA_NM'])

                print("strUniqueKey", strUniqueKey, type(strUniqueKey))

                qrySelectSeoulSwitch = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusDataTable + " WHERE unique_key=%s  "

                cursorRealEstate.execute(qrySelectSeoulSwitch, str(strUniqueKey))
                nResultCount = cursorRealEstate.rowcount
                # print("nResultCount", nResultCount, type(nResultCount))

                if nResultCount > 0:
                    # UPDATE
                    SwitchDataList = cursorRealEstate.fetchone()
                    nSequence = str(SwitchDataList.get('seq'))
                    strStateCode = str(SwitchDataList.get('state'))

                    qryInfoUpdate = " UPDATE " + ConstRealEstateTable_GOV.SeoulBusDataTable + " SET "
                    qryInfoUpdate += " modify_date=now()"
                    qryInfoUpdate += " WHERE seq='" + nSequence + "' "
                    print("qryInfoUpdate", qryInfoUpdate, type(qryInfoUpdate))
                    print("strUniqueKey", strUniqueKey, type(strUniqueKey))
                    cursorRealEstate.execute(qryInfoUpdate)
                    ResRealEstateConnection.commit()

                else:
                    # INSERT
                    qryInfoInsert  = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulBusDataTable + " SET "
                    qryInfoInsert += " unique_key = '" + str(strUniqueKey) + "', "

                    for dictSeoulAptInfoDataMaster in dictSeoulRealtyTradeDataMaster.keys():

                        # print("dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster]", dictSeoulAptInfoDataMaster, dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster], type(dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster]))
                        # fields 타입이 int 면 empty 인 경우 예외 처리 - 타입오류 예방
                        if dictSeoulColumnInfoData[dictSeoulAptInfoDataMaster]['type'] == 'int' and dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] == '':
                            dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] = '0'

                        if dictSeoulColumnInfoData[dictSeoulAptInfoDataMaster]['type'] == 'datetime' and dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] == '':
                            dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] = '0000-00-00 00:00:00'

                        qryInfoInsert += " " + dictSeoulAptInfoDataMaster + "='" + dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] + "', "

                    qryInfoInsert += "modify_date=NOW(),"
                    qryInfoInsert += "reg_date=NOW()"
                    print("qryInfoInsert", qryInfoInsert, type(qryInfoInsert))
                    cursorRealEstate.execute(qryInfoInsert)

                    ResRealEstateConnection.commit()

                    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                    dictSwitchData = dict()
                    dictSwitchData['result'] = '10'
                    dictSwitchData['data_1'] = dictSeoulRealtyTradeDataMaster['USE_DT']
                    dictSwitchData['data_2'] = dictSeoulRealtyTradeDataMaster['BUS_ROUTE_NM']
                    dictSwitchData['data_3'] = dictSeoulRealtyTradeDataMaster['BUS_STA_NM']
                    dictSwitchData['data_4'] = nBaseDate
                    dictSwitchData['data_5'] = nStartNumber
                    dictSwitchData['data_6'] = nEndNumber

                    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
                #if nResultCount > 0: End
            # for list in jsonRowDatas:
            print("for list in jsonRowDatas: End", "====================================================")
            nStartNumber = nEndNumber + 1
            nEndNumber = nEndNumber + nProcessedCount
        # while True END

    #while True END

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_1'] = dictSeoulRealtyTradeDataMaster['USE_DT']
    dictSwitchData['data_2'] = dictSeoulRealtyTradeDataMaster['BUS_ROUTE_NM']
    dictSwitchData['data_3'] = dictSeoulRealtyTradeDataMaster['BUS_STA_NM']
    dictSwitchData['data_4'] = nBaseDate
    dictSwitchData['data_5'] = nStartNumber
    dictSwitchData['data_6'] = nEndNumber

    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


except Exception as e:

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '30'
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    print(GetLogDef.lineno(__file__), "Error Exception")
    err_msg = traceback.format_exc()
    print(err_msg)
    print(e)
    print(type(e))

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


else:
    print("========================= SUCCESS END")

finally:
    print("Finally END")
    ResRealEstateConnection.close()
