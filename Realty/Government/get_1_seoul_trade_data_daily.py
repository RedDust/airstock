# 서울 일별 실거래가 데이터 수집 프로그램
# 2023-01-30 커밋
# https://data.seoul.go.kr/dataList/OA-21275/S/1/datasetView.do


import sys
sys.path.append("D:/PythonProjects/airstock")
#https://data.seoul.go.kr/dataList/OA-21275/S/1/datasetView.do


import urllib.request
import json
import pymysql
import datetime

import pandas as pd
from pandas.io.json import json_normalize
from Realty.Government.Init import init_conf
from Lib.RDB import pyMysqlConnector

from SeoulLib.RDB import LibSeoulRealTradeSwitch

from Init.Functions.Logs import GetLogDef

from Realty.Government.Const import ConstRealEstateTable_GOV

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable


try:

    #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
    #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
    #거래 신고 30일 + 취소 신고 +30일
    stToday = DateTime.today()

    nInsertedCount = 0
    nUpdateCount = 0

    #서울 부동산 실거래가 데이터 - 매매
    strProcessType = '034100'
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
    dictSwitchData['data_1'] = KuIndex
    dictSwitchData['data_2'] = arrCityPlace
    dictSwitchData['data_3'] = targetRow
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


    for nLoop in range(0, 61):
        nbaseDate = stToday - TimeDelta(days=nLoop)
        dtProcessDay = int(nbaseDate.strftime("%Y%m%d"))

        # 한번에 처리할 건수
        nProcessedCount = 1000

        nTotalCount = 0

        # 실제[]
        nLoopTotalCount = 0

        # 시작번호
        nStartNumber = 1

        # 최종번호
        nEndNumber = nProcessedCount

        strProcessDay = str(dtProcessDay)
        strProcessYear = strProcessDay[0:4]


        print( GetLogDef.lineno(), "dtProcessDay >", dtProcessDay , "strProcessYear > ", strProcessYear )


        while True:



            # 시작번호가 총 카운트 보다 많으면 중단
            if (nTotalCount > 0) and (nStartNumber > nTotalCount):
                break

            print(GetLogDef.lineno(), "nStartNumber >", nStartNumber)
            print(GetLogDef.lineno(), "nEndNumber >", nEndNumber)

            # # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            # url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/tbLnOpendataRtmsV/"+str(nStartNumber)+"/"+str(nEndNumber)+"/"+strProcessYear

            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/tbLnOpendataRtmsV/"+str(nStartNumber)+"/"+str(nEndNumber) \
                  + "/%20/%20/%20/%20/%20/%20/%20/%20/%20/" + strProcessDay + "/%20/"

            print("url > ", url)

            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            response = urllib.request.urlopen(url)
            json_str = response.read().decode("utf-8")

            # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
            json_object = json.loads(json_str)
            bMore = json_object.get('tbLnOpendataRtmsV')

            if bMore is None:
                Exception(GetLogDef.lineno(), 'bMore => ', bMore)  # 예외를 발생시킴
                break

            jsonResultDatas = bMore.get('RESULT')
            jsonResultDatasResult = jsonResultDatas

            strResultCode = jsonResultDatasResult.get('CODE')
            strResultMessage = jsonResultDatasResult.get('MESSAGE')

            if strResultCode != 'INFO-000':
                print(GetLogDef.lineno(), strResultCode, strResultMessage)
                break
                #GetOut while True:

            nTotalCount = bMore.get('list_total_count')
            jsonRowDatas = bMore.get('row')

            # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

            qrySelectSeoulSwitch = "SELECT * FROM "+ConstRealEstateTable_GOV.SeoulRealTradeMasterSwitchTable+" WHERE ACC_YEAR = %s AND state <> '30' LIMIT 1 "
            cursorRealEstate.execute(qrySelectSeoulSwitch, strProcessYear)

            nResultCount = cursorRealEstate.rowcount
            if nResultCount < 1:
                qrySwitchInsert = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulRealTradeMasterSwitchTable + " SET " \
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

            #Switch 업데이트
            dictSeoulSwitch = {}
            dictSeoulSwitch['seq'] = nSequence
            dictSeoulSwitch['state'] = 10
            print(GetLogDef.lineno(), "dictSeoulSwitch >", dictSeoulSwitch)
            bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulUpdate(dictSeoulSwitch)
            print(GetLogDef.lineno(), "bSwitchUpdateResult >", bSwitchUpdateResult)
            if bSwitchUpdateResult != True:
                Exception(GetLogDef.lineno(), 'SwitchData Not Allow')  # 예외를 발생시킴


            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = dtProcessDay
            dictSwitchData['data_2'] = strProcessDay
            dictSwitchData['data_3'] = nStartNumber
            dictSwitchData['data_4'] = nEndNumber
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            # 3. 건별 처리
            print("Processing", "====================================================")

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
                               dictSeoulRealtyTradeDataMaster['BONBEON'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['BUBEON'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['FLOOR'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['DEAL_YMD'] + "_" + dictSeoulRealtyTradeDataMaster['OBJ_AMT']

                print("strUniqueKey > ", strUniqueKey)

                cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
                qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + "  WHERE  unique_key=%s"

                cursorRealEstate.execute(qrySelectSeoulTradeMaster, strUniqueKey)
                row_result = cursorRealEstate.rowcount
                # 등록되어 있는 물건이면 패스

                if len(dictSeoulRealtyTradeDataMaster['CNTL_YMD']) > 5:
                    strState = "10"
                else:
                    strState = "00"

                if row_result > 0:

                    rstSelectDatas = cursorRealEstate.fetchone()

                    strCancelState = rstSelectDatas.get('state')
                    strCNTLYMD = rstSelectDatas.get('CNTL_YMD')

                    if(strCancelState != '10' and strState == '10' ):
                        sqlSeoulRealTrade = " UPDATE " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + " SET " \
                                            " CNTL_YMD='" + dictSeoulRealtyTradeDataMaster['CNTL_YMD'] + "'" \
                                          + " , state='" + strState + "' " \
                                          + " , modify_date=NOW() " \
                                          + " WHERE unique_key='"+strUniqueKey+"' "
                        nUpdateCount = nUpdateCount + 1
                        print("sqlSeoulRealTradeUpdate > ", sqlSeoulRealTrade)
                        cursorRealEstate.execute(sqlSeoulRealTrade)

                    else:
                     print("if(len(strCNTLYMD) < 1 ) END", strCNTLYMD)

                else:

                    sqlSeoulRealTrade = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + " SET unique_key='"+strUniqueKey+"' ," \
                                        " ACC_YEAR='" + dictSeoulRealtyTradeDataMaster['ACC_YEAR'] + "', " \
                                        " SGG_CD='" + dictSeoulRealtyTradeDataMaster['SGG_CD'] + "', " \
                                        " SGG_NM='" + dictSeoulRealtyTradeDataMaster['SGG_NM'] + "', " \
                                        " BJDONG_CD='" + dictSeoulRealtyTradeDataMaster['BJDONG_CD'] + "', " \
                                        " BJDONG_NM='" + dictSeoulRealtyTradeDataMaster['BJDONG_NM'] + "', " \
                                        " BONBEON='" + dictSeoulRealtyTradeDataMaster['BONBEON'] + "', " \
                                        " BUBEON='" + dictSeoulRealtyTradeDataMaster['BUBEON'] + "', " \
                                        " LAND_GBN='" + dictSeoulRealtyTradeDataMaster['LAND_GBN'] + "', " \
                                        " LAND_GBN_NM='" + dictSeoulRealtyTradeDataMaster['LAND_GBN_NM'] + "', " \
                                        " BLDG_NM='" + dictSeoulRealtyTradeDataMaster['BLDG_NM'].replace('\'', "\\'") + "', " \
                                        " HOUSE_TYPE='" + dictSeoulRealtyTradeDataMaster['HOUSE_TYPE'] + "', " \
                                        " DEAL_YMD='" + dictSeoulRealtyTradeDataMaster['DEAL_YMD'] + "', " \
                                        " OBJ_AMT='" + dictSeoulRealtyTradeDataMaster['OBJ_AMT'] + "', " \
                                        " BLDG_AREA='" + dictSeoulRealtyTradeDataMaster['BLDG_AREA'] + "', " \
                                        " TOT_AREA='" + dictSeoulRealtyTradeDataMaster['TOT_AREA'] + "', " \
                                        " FLOOR='" + dictSeoulRealtyTradeDataMaster['FLOOR'] + "', " \
                                        " RIGHT_GBN='" + dictSeoulRealtyTradeDataMaster['RIGHT_GBN'] + "', " \
                                        " CNTL_YMD='" + dictSeoulRealtyTradeDataMaster['CNTL_YMD'] + "', " \
                                        " BUILD_YEAR='" + dictSeoulRealtyTradeDataMaster['BUILD_YEAR'] + "', " \
                                        " state='" + strState + "', " \
                                        " REQ_GBN='" + dictSeoulRealtyTradeDataMaster['REQ_GBN'] + "', " \
                                        " RDEALER_LAWDNM='" + dictSeoulRealtyTradeDataMaster['ACC_YEAR'] + "' "

                    print("sqlSeoulRealTrade > ", sqlSeoulRealTrade)
                    cursorRealEstate.execute(sqlSeoulRealTrade)

                    nInsertedCount = nInsertedCount + 1

                ResRealEstateConnection.commit()

            # Switch 성공 업데이트
            dictSeoulSwitch = {}
            dictSeoulSwitch['seq'] = nSequence
            dictSeoulSwitch['state'] = 30
            dictSeoulSwitch['processed_count'] = nInsertedCount + nUpdateCount

            print(GetLogDef.lineno(), "dictSeoulSwitch >", dictSeoulSwitch)
            bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulUpdate(dictSeoulSwitch)
            print(GetLogDef.lineno(), "bSwitchUpdateResult >", bSwitchUpdateResult)



            #for list in jsonRowDatas:
            print("dictSeoulRealtyTradeDataMaster", "====================================================")
            nStartNumber = nEndNumber + 1
            nEndNumber = nEndNumber + nProcessedCount
            ResRealEstateConnection.close()

        #while True:
        print(GetLogDef.lineno(), "End While")
    # For nProcessYear:
    print(GetLogDef.lineno(), "End nProcessYears")

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_1'] = dtProcessDay
    dictSwitchData['data_2'] = strProcessDay
    dictSwitchData['data_3'] = nStartNumber
    dictSwitchData['data_4'] = nEndNumber
    dictSwitchData['data_5'] = nSequence
    dictSwitchData['data_6'] = nInsertedCount
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

except Exception as e:
    print("Error Exception")
    print(e)
    print(type(e))

    # Switch 오류 업데이트
    dictSeoulSwitch = {}
    dictSeoulSwitch['seq'] = nSequence
    dictSeoulSwitch['state'] = 20
    print(GetLogDef.lineno(), "dictSeoulSwitch >", dictSeoulSwitch)
    bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulUpdate(dictSeoulSwitch)
    print(GetLogDef.lineno(), "bSwitchUpdateResult >", bSwitchUpdateResult)

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '30'
    if dtProcessDay is not None:
        dictSwitchData['data_1'] = dtProcessDay

    if strProcessDay is not None:
        dictSwitchData['data_2'] = strProcessDay

    if nStartNumber is not None:
        dictSwitchData['data_3'] = nStartNumber

    if nEndNumber is not None:
        dictSwitchData['data_3'] = nEndNumber

    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)




else:
    print('Inserted => ', nInsertedCount, ' , Updated => ', nUpdateCount)
    print("========================= SUCCESS END")
finally:
    print("Finally END")



