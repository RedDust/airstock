# 서울시 지하철 사용량
#
# 2023-03-28 커밋
#https://data.seoul.go.kr/dataList/OA-12914/S/1/datasetView.do

import sys
sys.path.append("D:/PythonProjects/airstock")


import urllib.request
import json
import pymysql
import datetime

import pandas as pd
from pandas.io.json import json_normalize
from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from Lib.RDB import pyMysqlConnector


from SeoulLib.RDB import LibSeoulRealTradeSwitch

from Init.Functions.Logs import GetLogDef


from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable

# 한번에 처리할 건수
nProcessedCount = 1000

nTotalCount = 0

# 실제[]
nLoopTotalCount = 0

# 시작번호
nStartNumber = 1

# 최종번호
nEndNumber = nProcessedCount

try:

    #서울 부동산 실거래가 데이터 - 서울 지하철 사용량
    strProcessType = '034180'
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


    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '10'
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

    #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
    #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
    #거래 신고 30일 + 취소 신고 +30일
    dtToday = DateTime.today()
    # nBaseProcessDate = 50004

    nBaseProcessDate = 3

    nEmptyCount = 0

    nCount = 0

    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

    qrySelectSeoulTradeMaster = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulSubwayDataTable + " ORDER BY seq DESC LIMIT 1"

    cursorRealEstate.execute(qrySelectSeoulTradeMaster)
    SwitchDataList = cursorRealEstate.fetchone()
    # print(GetLogDef.lineno(__file__),SwitchDataList)
    dtUSE_DT = int(SwitchDataList.get('USE_DT'))


    while True:

        end_date = dtToday - TimeDelta(days=nBaseProcessDate)
        nBaseProcessDate = nBaseProcessDate + 1

        nBaseDate = int(end_date.strftime('%Y%m%d'))

        print(GetLogDef.lineno(), "nBaseStartDate > ", nBaseDate)
        nCount = nCount + 1
        if nCount > 5:
            break

        #
        # if dtUSE_DT > nBaseDate:
        #     break




        # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
        url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/CardSubwayStatsNew/1/"+str(nProcessedCount)+"/" +str(nBaseDate)

        print(GetLogDef.lineno(__file__), "url > ", url)

        # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
        response = urllib.request.urlopen(url)
        json_str = response.read().decode("utf-8")

        # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
        json_object = json.loads(json_str)

        if json_object.get('RESULT') != None:
            print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('CODE'))
            print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('MESSAGE'))
            # Exception(GetLogDef.lineno(__file__) + "json_object.RESULT => Not None")  # 예외를 발생시킴
            nEmptyCount = nEmptyCount + 1

            if nEmptyCount > 10:
                break
            else:
                continue

        bMore = json_object.get('CardSubwayStatsNew')

        if bMore is None:
            Exception(GetLogDef.lineno(__file__), 'bMore => ', bMore)  # 예외를 발생시킴

        nTotalCount = bMore.get('list_total_count')
        jsonResultDatas = bMore.get('RESULT')
        jsonResultDatasResult = jsonResultDatas

        strResultCode = jsonResultDatasResult.get('CODE')
        strResultMessage = jsonResultDatasResult.get('MESSAGE')

        if strResultCode != 'INFO-000':
            Exception(GetLogDef.lineno(), 'strResultCode => ', strResultCode)  # 예외를 발생시킴
            # GetOut while True:

        jsonRowDatas = bMore.get('row')

        print("Processing", "====================================================")
        nInsertedCount = 0
        nUpdateCount = 0
        nLoop = 0
        strUniqueKey = ''
        insertDict = dict()

        for jsonRowData in jsonRowDatas:
            print("[",jsonRowData ,"][ " + str(nLoop) + " ] ")
            nLoop += 1
            print(GetLogDef.lineno(__file__), "====================================================")
            USE_DT = jsonRowData.get('USE_DT')
            LINE_NUM = jsonRowData.get('LINE_NUM')
            SUB_STA_NM = jsonRowData.get('SUB_STA_NM')
            RIDE_PASGR_NUM = str(jsonRowData.get('RIDE_PASGR_NUM'))
            ALIGHT_PASGR_NUM = str(jsonRowData.get('ALIGHT_PASGR_NUM'))
            WORK_DT = jsonRowData.get('WORK_DT')

            dtBaseYear = int(USE_DT[0:4])
            dtBaseMonth = int(USE_DT[4:6])
            dtBaseDay = int(USE_DT[6:8])

            rstUseDate = datetime.date(dtBaseYear, dtBaseMonth, dtBaseDay)
            # rstUseDate = datetime.date(2023, 3, 26)
            print(GetLogDef.lineno(__file__), "====================================================")
            nWeekDay = rstUseDate.weekday()

            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
            qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulSubwayDataTable + "  WHERE  USE_DT=%s AND LINE_NUM=%s AND SUB_STA_NM=%s"

            print(GetLogDef.lineno(__file__), qrySelectSeoulTradeMaster)
            print(GetLogDef.lineno(__file__), USE_DT, LINE_NUM, SUB_STA_NM)
            cursorRealEstate.execute(qrySelectSeoulTradeMaster, (USE_DT, LINE_NUM, SUB_STA_NM))

            print(GetLogDef.lineno(__file__), "====================================================")
            row_result = cursorRealEstate.rowcount
            # 등록되어 있는 물건이면 패스

            # 이미 저장 되어 있으면 더이상 저장 하지 않는다.
            if row_result > 0:
                continue

            sqlSeoulSubway = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulSubwayDataTable + " SET " \
                                " USE_DT=%s" \
                                " , day_of_week =%s " \
                                " , LINE_NUM=%s" \
                                " , SUB_STA_NM=%s" \
                                " , RIDE_PASGR_NUM=%s" \
                                " , ALIGHT_PASGR_NUM=%s" \
                                " , WORK_DT=%s" \

            insertDict = (USE_DT, nWeekDay, LINE_NUM, SUB_STA_NM, RIDE_PASGR_NUM, ALIGHT_PASGR_NUM, WORK_DT)

            print(GetLogDef.lineno(__file__), "====================================================")

            cursorRealEstate.execute(sqlSeoulSubway, insertDict)
            ResRealEstateConnection.commit()

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = USE_DT
        dictSwitchData['data_2'] = LINE_NUM
        dictSwitchData['data_3'] = SUB_STA_NM
        dictSwitchData['data_4'] = RIDE_PASGR_NUM
        dictSwitchData['data_4'] = ALIGHT_PASGR_NUM
        dictSwitchData['data_4'] = WORK_DT
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    ResRealEstateConnection.close()
    #while True END

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_1'] = USE_DT
    dictSwitchData['data_2'] = LINE_NUM
    dictSwitchData['data_3'] = SUB_STA_NM
    dictSwitchData['data_4'] = RIDE_PASGR_NUM
    dictSwitchData['data_4'] = ALIGHT_PASGR_NUM
    dictSwitchData['data_4'] = WORK_DT
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


except Exception as e:

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '30'
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    print(GetLogDef.lineno(__file__), "Error Exception")
    print(e)
    print(type(e))

else:
    print("========================= SUCCESS END")

finally:
    print("Finally END")

