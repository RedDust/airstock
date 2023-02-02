# 서울시 건설공사 추진 현황
# 서울시(투출, 자치구 포함)에서 발주 한 시설공사 공정관리 및 일반공사 정보로 공정율, 사업비, 공사기간, 사업규모, 계약현황 등이 제공됩니다.
# 2023-01-30 커밋
#https://data.seoul.go.kr/dataList/OA-21276/S/1/datasetView.do


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

    while True:

        print(GetLogDef.lineno(), "nStartNumber >", nStartNumber)
        print(GetLogDef.lineno(), "nEndNumber >", nEndNumber)

        # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
        url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/ListOnePMISBizInfo/" + str(
            nStartNumber) + "/" + str(nEndNumber)

        print("url > ", url)

        # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
        response = urllib.request.urlopen(url)
        json_str = response.read().decode("utf-8")

        # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
        json_object = json.loads(json_str)
        bMore = json_object.get('ListOnePMISBizInfo')

        if bMore is None:
            Exception(GetLogDef.lineno(), 'bMore => ', bMore)  # 예외를 발생시킴
            break

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        nTotalCount = bMore.get('list_total_count')
        jsonResultDatas = bMore.get('RESULT')
        jsonResultDatasResult = jsonResultDatas

        strResultCode = jsonResultDatasResult.get('CODE')
        strResultMessage = jsonResultDatasResult.get('MESSAGE')

        if strResultCode != 'INFO-000':
            Exception(GetLogDef.lineno(), 'strResultCode => ', strResultCode)  # 예외를 발생시킴
            # GetOut while True:

        jsonRowDatas = bMore.get('row')

        # 3. 건별 처리
        print("Processing", "====================================================")
        nInsertedCount = 0
        nUpdateCount = 0
        nLoop = 0
        strUniqueKey = ''

        for list in jsonRowDatas:
            print("[ " + str(nStartNumber) + " - " + str(nEndNumber) + " ][ " + str(nLoop) + " ] ")
            nLoop += 1

            dictSeoulRealtyTradeDataMaster = {}

            for dictSeoulMasterKeys in list.keys():

                for dictNaverMobileMasterKeys in list.keys():
                    dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = list.get(dictSeoulMasterKeys)

                # Non-strings are converted to strings.
                if type(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys]) is not str:
                    dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = str(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys])


            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
            qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulSisulDataTable + "  WHERE  PJT_CD=%s"

            cursorRealEstate.execute(qrySelectSeoulTradeMaster, dictSeoulRealtyTradeDataMaster['PJT_CD'])
            row_result = cursorRealEstate.rowcount
            #이미 저장 되어 있으면 더이상 저장 하지 않는다.
            if row_result > 0:
                continue

            arrCNTRCT_PRD = []
            # 계약기간이 명시 되어 있지 않으면 예외처리
            if len(dictSeoulRealtyTradeDataMaster['DU_DATE']) < 2:
                arrCNTRCT_PRD = [str(0000-00-00), str(0000-00-00)]
            else:
                arrCNTRCT_PRD = dictSeoulRealtyTradeDataMaster['DU_DATE'].split("~")

            sqlSeoulRealTrade = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulSisulDataTable + " SET " \
                                " PJT_CD='" + dictSeoulRealtyTradeDataMaster['PJT_CD'] + "', " \
                                " PJT_NAME='" + dictSeoulRealtyTradeDataMaster['PJT_NAME'].replace('\'', "\\'") + "', " \
                                " PJT_FIN_YN='" + dictSeoulRealtyTradeDataMaster['PJT_FIN_YN'] + "', " \
                                " PJT_FIN_YN_NM='" + dictSeoulRealtyTradeDataMaster['PJT_FIN_YN_NM'] + "', " \
                                " PLAN_RT='" + dictSeoulRealtyTradeDataMaster['PLAN_RT'] + "', " \
                                " REAL_RT='" + dictSeoulRealtyTradeDataMaster['REAL_RT'] + "', " \
                                " PER_RT='" + dictSeoulRealtyTradeDataMaster['PER_RT'] + "', " \
                                " BASIC_DT='" + dictSeoulRealtyTradeDataMaster['BASIC_DT'] + "', " \
                                " DT1='" + dictSeoulRealtyTradeDataMaster['DT1'] + "', " \
                                " DT2='" + dictSeoulRealtyTradeDataMaster['DT2'] + "', " \
                                " DT3='" + dictSeoulRealtyTradeDataMaster['DT3'] + "', " \
                                " TOT_CNTRT_AMT='" + dictSeoulRealtyTradeDataMaster['TOT_CNTRT_AMT'] + "', " \
                                " TOT_PJT_AMT='" + dictSeoulRealtyTradeDataMaster['TOT_PJT_AMT'] + "', " \
                                " DU_DATE_START='" + arrCNTRCT_PRD[0] + "', " \
                                " DU_DATE_END='" + arrCNTRCT_PRD[1] + "', " \
                                " OFFICE_ADDR='" + dictSeoulRealtyTradeDataMaster['OFFICE_ADDR'].replace('\'', "\\'") + "', " \
                                " LAT='" + dictSeoulRealtyTradeDataMaster['LAT'] + "', " \
                                " LNG='" + dictSeoulRealtyTradeDataMaster['LNG'] + "', " \
                                " USER_1='" + dictSeoulRealtyTradeDataMaster['USER_1'].replace('\'', "\\'") + "', " \
                                " USER_2='" + dictSeoulRealtyTradeDataMaster['USER_2'].replace('\'', "\\'") + "', " \
                                " USER_3='" + dictSeoulRealtyTradeDataMaster['USER_3'].replace('\'', "\\'") + "', " \
                                " ORG_1='" + dictSeoulRealtyTradeDataMaster['ORG_1'].replace('\'', "\\'") + "', " \
                                " ORG_2='" + dictSeoulRealtyTradeDataMaster['ORG_2'].replace('\'', "\\'") + "', " \
                                " ORG_3='" + dictSeoulRealtyTradeDataMaster['ORG_3'].replace('\'', "\\'") + "', " \
                                " PJT_SCALE='" + dictSeoulRealtyTradeDataMaster['PJT_SCALE'].replace('\'', "\\'") + "', " \
                                " RTSP_ADDR='" + dictSeoulRealtyTradeDataMaster['RTSP_ADDR'].replace('\'', "\\'") + "', " \
                                " CNRT_ADDR='" + dictSeoulRealtyTradeDataMaster['CNRT_ADDR'].replace('\'', "\\'") + "', " \
                                " BILLPAY_ADDR='" + dictSeoulRealtyTradeDataMaster['BILLPAY_ADDR'].replace('\'', "\\'") + "', " \
                                " AIR_VIEW_IMG='" + dictSeoulRealtyTradeDataMaster['AIR_VIEW_IMG'].replace('\'', "\\'") + "', " \
                                " ALIMI_ADDR='" + dictSeoulRealtyTradeDataMaster['ALIMI_ADDR'].replace('\'', "\\'") + "' "

            print("sqlSeoulRealTrade > ", sqlSeoulRealTrade)
            cursorRealEstate.execute(sqlSeoulRealTrade)
            nInsertedCount = nInsertedCount + 1

            ResRealEstateConnection.commit()


        nStartNumber = nEndNumber + 1
        nEndNumber = nEndNumber + nProcessedCount
        ResRealEstateConnection.close()

except Exception as e:


    print("Error Exception")
    print(e)
    print(type(e))


else:
    print("========================= SUCCESS END")

finally:
    print("Finally END")

