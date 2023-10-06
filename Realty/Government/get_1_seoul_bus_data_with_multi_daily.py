#이게 날짜별 멀티 프로세싱
#https://data.seoul.go.kr/dataList/OA-12912/S/1/datasetView.do

# quit("500")

import os
import time
import datetime
import math
import traceback
import urllib.request
import json
import pymysql

from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.RDB import pyMysqlConnector
from multiprocessing import Process
from Init.Functions.Logs import GetLogDef
from Lib.CustomException import QuitException
from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable


import get_1_seoul_bus_data_with_thread_child

if __name__ == '__main__':

    try:

        print("Process START["+str(DateTime.today()) +"]=======================================================")

        # nTotal = 46061

        # 서울 부동산 실거래가 데이터 - 서울 버스 사용량
        strProcessType = '034170'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'

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

        nBaseProcessDate = 0
        nStartNumber = 1
        nEndNumber = 2
        nProcessingPerCount = 1000
        nTotalCount = 0
        # nFinalDate = 20230101 #포함해서 처리 한다.

        # #데일리 프로세스를 위한 변수 START
        # dtToday = DateTime.today()
        # # 데일리 프로세스를 위한 변수 END


        #일괄 프로세스를 위한 변수 START
        # dtToday = datetime.datetime(2015, 12, 31, 00, 00, 00)
        dtToday = DateTime.today()

        # 일괄 프로세스를 위한 변수 END

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        strInsertYear = str(dtToday.year)

        #
        qrySelectLastProcessDate = "SELECT USE_DT FROM kt_realty_seoul_bus_using_static_"+strInsertYear+" order by USE_DT desc limit 1 "
        cursorRealEstate.execute(qrySelectLastProcessDate)
        SelectUseDT = cursorRealEstate.fetchone()

        print(GetLogDef.lineno(__file__), type(SelectUseDT['USE_DT']), SelectUseDT['USE_DT'])
        nFinalDate = int(SelectUseDT['USE_DT'])


        dictSeoulColumnInfoData = {}

        # 필드 테이블 컬럼 타입 조회 int 인경우는 예외처리 하기 위한 준비
        qrySelectSeoulTableColumns = "show columns from kt_realty_seoul_bus_using_static_"+strInsertYear+" "
        cursorRealEstate.execute(qrySelectSeoulTableColumns)
        SelectColumnLists = cursorRealEstate.fetchall()




        for SelectColumnList in SelectColumnLists:
            strFieldName = str(SelectColumnList.get('Field'))
            strColumnType = str(SelectColumnList.get('Type'))
            dictSeoulColumnInfoData[strFieldName] = {}
            dictSeoulColumnInfoData[strFieldName]['name'] = strFieldName
            dictSeoulColumnInfoData[strFieldName]['type'] = strColumnType

            print(GetLogDef.lineno(__file__), type(SelectColumnList), SelectColumnList)

        print(GetLogDef.lineno(__file__), type(dictSeoulColumnInfoData), dictSeoulColumnInfoData)


        dictSeoulRealtyTradeDataMaster = {}

        while True:

            end_date = dtToday - TimeDelta(days=nBaseProcessDate)
            nBaseDate = int(end_date.strftime('%Y%m%d'))

            if nFinalDate >= nBaseDate:
                print(GetLogDef.lineno(__file__), "finalDate > ", nFinalDate, nBaseDate)
                break

            # 검색기준일을 미리 1 올려 놓는다.
            nBaseProcessDate += 1

            print(GetLogDef.lineno(__file__), type(end_date), end_date )

            print("Multi START[" + str(DateTime.today()) + "]-------------------------------------------------------------------")

            url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/CardBusStatisticsServiceNew/" + str(nStartNumber) + "/" + str(nEndNumber) + "/" + str(nBaseDate)
            # print(GetLogDef.lineno(__file__), "url > ", url)

            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            response = urllib.request.urlopen(url)
            json_str = response.read().decode("utf-8")

            # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
            json_object = json.loads(json_str)

            print(GetLogDef.lineno(__file__), "json_object.get('RESULT')  > ", json_object.get('RESULT'))

            # 조회된 데이터가 "있으면" json_object.get('RESULT') => None
            if json_object.get('RESULT') != None:
                print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('CODE'))
                print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('MESSAGE'))
                Exception(GetLogDef.lineno(__file__))  # 예외를 발생시킴

            if json_object.get('RESULT') != None and json_object.get('RESULT').get('CODE') == 'INFO-200':
                print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('CODE'))
                print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('MESSAGE'))
                continue

            bMore = json_object.get('CardBusStatisticsServiceNew')
            print(GetLogDef.lineno(__file__), type(bMore), bMore)

            if bMore is None:
                Exception(GetLogDef.lineno(__file__))  # 예외를 발생시킴

            nTotalCount = bMore.get('list_total_count')
            print(GetLogDef.lineno(__file__), type(nTotalCount), nTotalCount)

            nLoopCount = math.ceil(nTotalCount / nProcessingPerCount)

            print(GetLogDef.lineno(), "nBaseDate >", type(nBaseDate), nBaseDate)
            print(GetLogDef.lineno(), "nTotalCount >", type(nTotalCount), nTotalCount)
            print(GetLogDef.lineno(), "nProcessingPerCount >", type(nProcessingPerCount), nProcessingPerCount)
            print(GetLogDef.lineno(), "nLoopCount >", type(nLoopCount), nLoopCount)

            procs = []
            for nLoop in range(0, nLoopCount):
                nCallStartCount = 1 + (nLoop * nProcessingPerCount)
                nCallEndCount = ((nLoop + 1) * nProcessingPerCount)

                Multiprocess = Process(target=get_1_seoul_bus_data_with_thread_child.test_thread, args=(nBaseDate, nCallStartCount, nCallEndCount,dictSeoulColumnInfoData))
                procs.append(Multiprocess)
                Multiprocess.start()

            for Multiprocess in procs:
                Multiprocess.join()


            print("Multi END[" + str(DateTime.today()) + "]-------------------------------------------------------------------")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = nBaseDate
        dictSwitchData['data_2'] = nTotalCount
        dictSwitchData['data_3'] = nFinalDate
        dictSwitchData['data_4'] = nBaseDate
        # dictSwitchData['data_5'] = nCallStartCount
        # dictSwitchData['data_6'] = nCallEndCount

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        print("Process END["+str(DateTime.today())+"]=======================================================")

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
        # ResRealEstateConnection.close()
