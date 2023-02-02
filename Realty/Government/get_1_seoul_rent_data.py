# 서울 일별 전월세 데이터 수집 프로그램
# 2023-01-30 커밋
#https://data.seoul.go.kr/dataList/OA-21276/S/1/datasetView.do


import sys
sys.path.append("D:/PythonProjects/airstock")


import urllib.request
import json
import pymysql
import datetime

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV


from Lib.RDB import pyMysqlConnector


from SeoulLib.RDB import LibSeoulRealTradeSwitch

from Init.Functions.Logs import GetLogDef


from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable


try:

    #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
    #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
    #거래 신고 30일 + 취소 신고 +30일
    date_1 = DateTime.today()
    end_date = date_1 - TimeDelta(days=60)
    nBaseStartYear = str(end_date.strftime('%Y'))

    end_date = date_1 + TimeDelta(days=365)
    nEndYear = int(end_date.strftime('%Y'))

    nBaseEndYear = int(date_1.strftime('%Y'))
    nYearLoop = 1


    for nProcessYear in range(2011, nEndYear):

        # 한번에 처리할 건수
        nProcessedCount = 1000

        nTotalCount = 0

        # 실제[]
        nLoopTotalCount = 0

        # 시작번호
        nStartNumber = 1

        # 최종번호
        nEndNumber = nProcessedCount

        strProcessYear = str(nProcessYear)

        print(GetLogDef.lineno(), "nProcessYear >", nProcessYear)

        while True:

            # 시작번호가 총 카운트 보다 많으면 중단
            if (nTotalCount > 0) and (nStartNumber > nTotalCount):
                break

            print(GetLogDef.lineno(), "nStartNumber >", nStartNumber)
            print(GetLogDef.lineno(), "nEndNumber >", nEndNumber)

            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/tbLnOpendataRentV/"+str(nStartNumber)+"/"+str(nEndNumber)+"/"+strProcessYear
            print("url > ", url)


            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            response = urllib.request.urlopen(url)
            json_str = response.read().decode("utf-8")

            # print(json_str)



            # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
            json_object = json.loads(json_str)
            bMore = json_object.get('tbLnOpendataRentV')

            if bMore is None:
                Exception(GetLogDef.lineno(), 'bMore => ', bMore)  # 예외를 발생시킴
                break

            nTotalCount = bMore.get('list_total_count')
            jsonResultDatas = bMore.get('RESULT')
            jsonResultDatasResult = jsonResultDatas

            strResultCode = jsonResultDatasResult.get('CODE')
            strResultMessage = jsonResultDatasResult.get('MESSAGE')

            if strResultCode != 'INFO-000':
                Exception(GetLogDef.lineno(), 'strResultCode => ', strResultCode)  # 예외를 발생시킴
                break
                #GetOut while True:


            jsonRowDatas = bMore.get('row')

            # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

            qrySelectSeoulSwitch = "SELECT * FROM "+ConstRealEstateTable_GOV.SeoulRealRentMasterSwitchTable+" WHERE ACC_YEAR = %s AND state <> '30' LIMIT 1 "
            cursorRealEstate.execute(qrySelectSeoulSwitch, strProcessYear)

            nResultCount = cursorRealEstate.rowcount
            if nResultCount < 1:
                qrySwitchInsert = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulRealRentMasterSwitchTable + " SET " \
                                    "ACC_YEAR='"+strProcessYear+"', " \
                                    "START_NUMBER='"+str(nStartNumber)+"', " \
                                    "END_NUMBER='" + str(nEndNumber) + "', "\
                                    "RESULT_CODE='"+str(strResultCode)+"', " \
                                    "RESULT_MESSAGE='"+str(strResultMessage)+"', " \
                                    "list_total_count='"+str(nTotalCount)+"' ," \
                                    "state='00' "
                cursorRealEstate.execute(qrySwitchInsert)
                nSequence = cursorRealEstate.lastrowid
                print(GetLogDef.lineno(), "qrySwitchInert >", qrySwitchInsert)

            else:
                SwitchDataList = cursorRealEstate.fetchone()
                nSequence = SwitchDataList.get('seq')
                strStateCode = SwitchDataList.get('state')
                START_NUMBER = SwitchDataList.get('START_NUMBER')
                END_NUMBER = SwitchDataList.get('END_NUMBER')
                nLoopTotalCount = SwitchDataList.get('total_processed_count')

            ResRealEstateConnection.commit()


            # print(nTotalCount)
            # print(jsonResultDatas)
            # quit("END")

            #Switch 업데이트
            dictSeoulSwitch = {}
            dictSeoulSwitch['seq'] = nSequence
            dictSeoulSwitch['state'] = 10
            print(GetLogDef.lineno(), "dictSeoulSwitch >", dictSeoulSwitch)
            bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulRentUpdate(dictSeoulSwitch)
            print(GetLogDef.lineno(), "bSwitchUpdateResult >", bSwitchUpdateResult)
            if bSwitchUpdateResult != True:
                Exception(GetLogDef.lineno(), 'SwitchData Not Allow')  # 예외를 발생시킴


            # 3. 건별 처리
            print("Processing", "====================================================")
            nInsertedCount = 0
            nUpdateCount = 0
            nLoop = 0
            strUniqueKey = ''

            for list in jsonRowDatas:
                print("[ "+str(nStartNumber)+" - "+str(nEndNumber)+" ][ "+str(nLoop)+" ] ")
                nLoop += 1

                # DB 연결

                dictSeoulRealtyTradeDataMaster = {}

                for dictSeoulMasterKeys in list.keys():

                    for dictNaverMobileMasterKeys in list.keys():
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = list.get(dictSeoulMasterKeys)

                    # Non-strings are converted to strings.
                    if type(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys]) is not str:
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = str(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys])


                # `ACC_YEAR`, `SGG_CD`, `BJDONG_CD`,BONBEON BUBEON , `FLOOR` `DEAL_YMD`, `OBJ_AMT`
                strUniqueKey = dictSeoulRealtyTradeDataMaster['ACC_YEAR'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['SGG_CD'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['BJDONG_CD'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['BOBN'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['BUBN'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['FLR_NO'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['CNTRCT_DE'] + "_" + dictSeoulRealtyTradeDataMaster['RENT_GTN']

                print("strUniqueKey > ", strUniqueKey)

                print(dictSeoulRealtyTradeDataMaster)


                cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
                qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulRealRentDataTable + "  WHERE  unique_key=%s"

                cursorRealEstate.execute(qrySelectSeoulTradeMaster, strUniqueKey)
                row_result = cursorRealEstate.rowcount
                # 등록되어 있는 물건이면 패스


                #이미 저장 되어 있으면 더이상 저장 하지 않는다.
                if row_result > 0:
                    continue

                print(dictSeoulRealtyTradeDataMaster['CNTRCT_PRD'] ,type( dictSeoulRealtyTradeDataMaster['CNTRCT_PRD']))

                arrCNTRCT_PRD = []
                # 계약기간이 명시 되어 있지 않으면 예외처리
                if len(dictSeoulRealtyTradeDataMaster['CNTRCT_PRD']) < 2:
                    arrCNTRCT_PRD = [str(00.0), str(00.0)]
                else:
                    arrCNTRCT_PRD = dictSeoulRealtyTradeDataMaster['CNTRCT_PRD'].split("~")


                print(arrCNTRCT_PRD ,type( arrCNTRCT_PRD))

                if len(dictSeoulRealtyTradeDataMaster['RENT_GTN']) < 1:
                    dictSeoulRealtyTradeDataMaster['RENT_GTN'] = '0'

                if len(dictSeoulRealtyTradeDataMaster['RENT_FEE']) < 1:
                    dictSeoulRealtyTradeDataMaster['RENT_FEE'] = '0'

                if len(dictSeoulRealtyTradeDataMaster['BEFORE_GRNTY_AMOUNT']) < 1:
                    dictSeoulRealtyTradeDataMaster['BEFORE_GRNTY_AMOUNT'] = '0'

                if len(dictSeoulRealtyTradeDataMaster['BEFORE_MT_RENT_CHRGE']) < 1:
                    dictSeoulRealtyTradeDataMaster['BEFORE_MT_RENT_CHRGE'] = '0'


                sqlSeoulRealTrade = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulRealRentDataTable + " SET unique_key='"+strUniqueKey+"' ," \
                                    " ACC_YEAR='" + dictSeoulRealtyTradeDataMaster['ACC_YEAR'] + "', " \
                                    " SGG_CD='" + dictSeoulRealtyTradeDataMaster['SGG_CD'] + "', " \
                                    " SGG_NM='" + dictSeoulRealtyTradeDataMaster['SGG_NM'] + "', " \
                                    " BJDONG_CD='" + dictSeoulRealtyTradeDataMaster['BJDONG_CD'] + "', " \
                                    " BJDONG_NM='" + dictSeoulRealtyTradeDataMaster['BJDONG_NM'] + "', " \
                                    " LAND_GBN='" + dictSeoulRealtyTradeDataMaster['LAND_GBN'] + "', " \
                                    " LAND_GBN_NM='" + dictSeoulRealtyTradeDataMaster['LAND_GBN_NM'] + "', " \
                                    " BOBN='" + dictSeoulRealtyTradeDataMaster['BOBN'] + "', " \
                                    " BUBN='" + dictSeoulRealtyTradeDataMaster['BUBN'] + "', " \
                                    " FLR_NO='" + dictSeoulRealtyTradeDataMaster['FLR_NO'] + "', " \
                                    " CNTRCT_DE='" + dictSeoulRealtyTradeDataMaster['CNTRCT_DE'] + "', " \
                                    " RENT_GBN='" + dictSeoulRealtyTradeDataMaster['RENT_GBN'] + "', " \
                                    " RENT_AREA='" + dictSeoulRealtyTradeDataMaster['RENT_AREA'] + "', " \
                                    " RENT_GTN='" + dictSeoulRealtyTradeDataMaster['RENT_GTN'] + "', " \
                                    " RENT_FEE='" + dictSeoulRealtyTradeDataMaster['RENT_FEE'] + "', " \
                                    " BLDG_NM='" + dictSeoulRealtyTradeDataMaster['BLDG_NM'].replace('\'', "\\'") + "', " \
                                    " BUILD_YEAR='" + dictSeoulRealtyTradeDataMaster['BUILD_YEAR'] + "', " \
                                    " HOUSE_GBN_NM='" + dictSeoulRealtyTradeDataMaster['HOUSE_GBN_NM'] + "', " \
                                    " CNTRCT_PRD_START='" + arrCNTRCT_PRD[0] + "', " \
                                    " CNTRCT_PRD_END='" + arrCNTRCT_PRD[1] + "', " \
                                    " NEW_RON_SECD='" + dictSeoulRealtyTradeDataMaster['NEW_RON_SECD'] + "', " \
                                    " CNTRCT_UPDT_RQEST_AT='" + dictSeoulRealtyTradeDataMaster['CNTRCT_UPDT_RQEST_AT'] + "', " \
                                    " BEFORE_GRNTY_AMOUNT='" + dictSeoulRealtyTradeDataMaster['BEFORE_GRNTY_AMOUNT'] + "', " \
                                    " BEFORE_MT_RENT_CHRGE='" + dictSeoulRealtyTradeDataMaster['BEFORE_MT_RENT_CHRGE'] + "' " \

                print("sqlSeoulRealTrade > ", sqlSeoulRealTrade)
                cursorRealEstate.execute(sqlSeoulRealTrade)
                nInsertedCount = nInsertedCount + 1

                ResRealEstateConnection.commit()

            # Switch 성공 업데이트
            dictSeoulSwitch = {}
            dictSeoulSwitch['seq'] = nSequence
            dictSeoulSwitch['state'] = 30
            dictSeoulSwitch['processed_count'] = nInsertedCount

            print(GetLogDef.lineno(), "dictSeoulSwitch >", dictSeoulSwitch)
            bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulRentUpdate(dictSeoulSwitch)
            print(GetLogDef.lineno(), "bSwitchUpdateResult >", bSwitchUpdateResult)

            #for list in jsonRowDatas:
            print('Inserted => ', nInsertedCount)
            print("dictSeoulRealtyTradeDataMaster", "====================================================")
            nStartNumber = nEndNumber + 1
            nEndNumber = nEndNumber + nProcessedCount
            ResRealEstateConnection.close()

        #while True:
        print(GetLogDef.lineno(), "End While")
        nProcessYear = nProcessYear + 1
    # For nProcessYear:
    print(GetLogDef.lineno(), "End nProcessYears")


except Exception as e:
    print("Error Exception")
    print(e)
    print(type(e))

    # Switch 오류 업데이트
    dictSeoulSwitch = {}
    dictSeoulSwitch['seq'] = nSequence
    dictSeoulSwitch['state'] = 20
    print(GetLogDef.lineno(), "dictSeoulSwitch >", dictSeoulSwitch)
    bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulRentUpdate(dictSeoulSwitch)
    print(GetLogDef.lineno(), "bSwitchUpdateResult >", bSwitchUpdateResult)

else:
    print("========================= SUCCESS END")
finally:
    print("Finally END")



