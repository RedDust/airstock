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
from Lib.GeoDataModule import GeoDataModule

try:
    #사용변수 초기화
    nSequence = 0

    #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
    #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
    #거래 신고 30일 + 취소 신고 +30일
    stToday = DateTime.today()

    nInsertedCount = 0
    nUpdateCount = 0

    #서울 부동산 실거래가 데이터 - 임대차
    strProcessType = '034200'
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


        print( GetLogDef.lineno(__file__), "dtProcessDay >", dtProcessDay , "strProcessYear > ", strProcessYear )

        while True:

            # 시작번호가 총 카운트 보다 많으면 중단
            if (nTotalCount > 0) and (nStartNumber > nTotalCount):
                break

            print(GetLogDef.lineno(__file__), "nStartNumber >", nStartNumber)
            print(GetLogDef.lineno(__file__), "nEndNumber >", nEndNumber)

            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/tbLnOpendataRentV/"+str(nStartNumber)+"/"+str(nEndNumber) \
                  + "/%20/%20/%20/%20/%20/%20/%20/" + strProcessDay + "/%20/%20/"

            print(GetLogDef.lineno(__file__), "url > ", url)


            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            response = urllib.request.urlopen(url)
            json_str = response.read().decode("utf-8")


            # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
            json_object = json.loads(json_str)
            bMore = json_object.get('tbLnOpendataRentV')

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
                Exception(GetLogDef.lineno(__file__), 'strResultCode => ', strResultCode)  # 예외를 발생시킴
                break
                #GetOut while True:


            jsonRowDatas = bMore.get('row')

            # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

            # 3. 건별 처리
            print("Processing", "====================================================")
            nLoop = 0
            strUniqueKey = ''

            for list in jsonRowDatas:
                # print("[ "+str(nStartNumber)+" - "+str(nEndNumber)+" ][ "+str(nLoop)+" ] ")
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

                # print("strUniqueKey > ", strUniqueKey)
                # print(dictSeoulRealtyTradeDataMaster)


                cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
                qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulRealRentDataTable + "  WHERE  unique_key=%s"

                cursorRealEstate.execute(qrySelectSeoulTradeMaster, strUniqueKey)
                row_result = cursorRealEstate.rowcount
                # 등록되어 있는 물건이면 패스


                #이미 저장 되어 있으면 더이상 저장 하지 않는다.
                if row_result > 0:
                    continue

                # print(dictSeoulRealtyTradeDataMaster['CNTRCT_PRD'] ,type( dictSeoulRealtyTradeDataMaster['CNTRCT_PRD']))

                arrCNTRCT_PRD = []
                # 계약기간이 명시 되어 있지 않으면 예외처리
                if len(dictSeoulRealtyTradeDataMaster['CNTRCT_PRD']) < 2:
                    arrCNTRCT_PRD = [str(00.0), str(00.0)]
                else:
                    arrCNTRCT_PRD = dictSeoulRealtyTradeDataMaster['CNTRCT_PRD'].split("~")


                # print(arrCNTRCT_PRD ,type( arrCNTRCT_PRD))

                if len(dictSeoulRealtyTradeDataMaster['RENT_GTN']) < 1:
                    dictSeoulRealtyTradeDataMaster['RENT_GTN'] = '0'

                if len(dictSeoulRealtyTradeDataMaster['RENT_FEE']) < 1:
                    dictSeoulRealtyTradeDataMaster['RENT_FEE'] = '0'

                if len(dictSeoulRealtyTradeDataMaster['BEFORE_GRNTY_AMOUNT']) < 1:
                    dictSeoulRealtyTradeDataMaster['BEFORE_GRNTY_AMOUNT'] = '0'

                if len(dictSeoulRealtyTradeDataMaster['BEFORE_MT_RENT_CHRGE']) < 1:
                    dictSeoulRealtyTradeDataMaster['BEFORE_MT_RENT_CHRGE'] = '0'

                strTradeDBMasterBOBN = str(dictSeoulRealtyTradeDataMaster['BOBN']).lstrip("0")
                strTradeDBMasterBUBEON = str(dictSeoulRealtyTradeDataMaster['BUBN']).lstrip("0")

                strGOVHouseType = str(dictSeoulRealtyTradeDataMaster['HOUSE_GBN_NM'])

                if strGOVHouseType != '단독다가구':

                    qrySelectSeoulRentMaster  = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulRealRentDataTable + " "
                    qrySelectSeoulRentMaster += " WHERE SGG_NM=%s "
                    qrySelectSeoulRentMaster += " AND BJDONG_NM=%s "
                    qrySelectSeoulRentMaster += " AND BOBN=%s "
                    qrySelectSeoulRentMaster += " AND BUBN=%s "
                    qrySelectSeoulRentMaster += " AND lat!='' "
                    qrySelectSeoulRentMaster += " AND lng!='' "
                    qrySelectSeoulRentMaster += " LIMIT 1 "

                    cursorRealEstate.execute(qrySelectSeoulRentMaster, (dictSeoulRealtyTradeDataMaster['SGG_NM'],dictSeoulRealtyTradeDataMaster['BJDONG_NM'],dictSeoulRealtyTradeDataMaster['BOBN'] ,dictSeoulRealtyTradeDataMaster['BUBN'] ))
                    intSelectResult = cursorRealEstate.rowcount
                    if intSelectResult > 0:
                        rstSelectData = cursorRealEstate.fetchone()
                        strCancelState = rstSelectData.get('state')
                        strNaverLongitude = str(rstSelectData.get('lng'))  # 127
                        strNaverLatitude = str(rstSelectData.get('lat'))  # 37
                    else:

                        strTradeDBMasterBOBN = strTradeDBMasterBOBN
                        if strTradeDBMasterBUBEON != '':
                            strTradeDBMasterBUBEON = "-" + strTradeDBMasterBUBEON

                        strDOROJUSO = "서울특별시 "
                        strDOROJUSO += dictSeoulRealtyTradeDataMaster['SGG_NM'] + " "
                        strDOROJUSO += dictSeoulRealtyTradeDataMaster['BJDONG_NM'] + " "
                        strDOROJUSO += strTradeDBMasterBOBN
                        strDOROJUSO += strTradeDBMasterBUBEON
                        print(GetLogDef.lineno(__file__), "strDOROJUSO => ", strDOROJUSO)

                        resultsDict = GeoDataModule.getJusoData(strDOROJUSO)
                        print(GetLogDef.lineno(__file__), "resultsDict >", type(resultsDict), isinstance(resultsDict, dict), resultsDict)

                        if isinstance(resultsDict, dict) == False:
                            print(GetLogDef.lineno(__file__), "resultsDict >", type(resultsDict), resultsDict)
                        else:
                            print(GetLogDef.lineno(__file__), resultsDict['jibunAddr'])
                            strDOROJUSO = str(resultsDict['roadAddrPart1']).strip()

                        resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
                        print(GetLogDef.lineno(__file__), "resultsDict >", type(resultsDict), resultsDict)
                        if isinstance(resultsDict, dict) == False:
                            print(GetLogDef.lineno(__file__), "resultsDict >", type(resultsDict), resultsDict)
                            raise Exception(GetLogDef.lineno(__file__) + 'strResultCode => ' + strResultCode)  # 예외를 발생시킴
                            break

                        strNaverLongitude = str(resultsDict['x'])  # 127
                        strNaverLatitude = str(resultsDict['y'])  # 37

                else:
                    strNaverLongitude = str(0)
                    strNaverLatitude = str(0)


                sqlSeoulRealTrade = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulRealRentDataTable + " SET unique_key='"+strUniqueKey+"' ,"
                sqlSeoulRealTrade += " ACC_YEAR='" + dictSeoulRealtyTradeDataMaster['ACC_YEAR'] + "', "
                sqlSeoulRealTrade += " SGG_CD='" + dictSeoulRealtyTradeDataMaster['SGG_CD'] + "', "
                sqlSeoulRealTrade += " SGG_NM='" + dictSeoulRealtyTradeDataMaster['SGG_NM'] + "', "
                sqlSeoulRealTrade += " BJDONG_CD='" + dictSeoulRealtyTradeDataMaster['BJDONG_CD'] + "', "
                sqlSeoulRealTrade += " BJDONG_NM='" + dictSeoulRealtyTradeDataMaster['BJDONG_NM'] + "', "
                sqlSeoulRealTrade += " LAND_GBN='" + dictSeoulRealtyTradeDataMaster['LAND_GBN'] + "', "
                sqlSeoulRealTrade += " LAND_GBN_NM='" + dictSeoulRealtyTradeDataMaster['LAND_GBN_NM'] + "', "
                sqlSeoulRealTrade += " BOBN='" + dictSeoulRealtyTradeDataMaster['BOBN'] + "', "
                sqlSeoulRealTrade += " BUBN='" + dictSeoulRealtyTradeDataMaster['BUBN'] + "', "
                sqlSeoulRealTrade += " lng='" + strNaverLongitude + "', "
                sqlSeoulRealTrade += " lat='" + strNaverLatitude + "', "
                sqlSeoulRealTrade += " geo_point = ST_GeomFromText('POINT(" + strNaverLongitude + " " + strNaverLatitude + ")', 4326,'axis-order=long-lat'), "
                sqlSeoulRealTrade += " FLR_NO='" + dictSeoulRealtyTradeDataMaster['FLR_NO'] + "', "
                sqlSeoulRealTrade += " CNTRCT_DE='" + dictSeoulRealtyTradeDataMaster['CNTRCT_DE'] + "', "
                sqlSeoulRealTrade += " RENT_GBN='" + dictSeoulRealtyTradeDataMaster['RENT_GBN'] + "', "
                sqlSeoulRealTrade += " RENT_AREA='" + dictSeoulRealtyTradeDataMaster['RENT_AREA'] + "', "
                sqlSeoulRealTrade += " RENT_GTN='" + dictSeoulRealtyTradeDataMaster['RENT_GTN'] + "', "
                sqlSeoulRealTrade += " RENT_FEE='" + dictSeoulRealtyTradeDataMaster['RENT_FEE'] + "', "
                sqlSeoulRealTrade += " BLDG_NM='" + dictSeoulRealtyTradeDataMaster['BLDG_NM'].replace('\'', "\\'") + "', "
                sqlSeoulRealTrade += " BUILD_YEAR='" + dictSeoulRealtyTradeDataMaster['BUILD_YEAR'] + "', "
                sqlSeoulRealTrade += " HOUSE_GBN_NM='" + dictSeoulRealtyTradeDataMaster['HOUSE_GBN_NM'] + "', "
                sqlSeoulRealTrade += " CNTRCT_PRD_START='" + arrCNTRCT_PRD[0] + "', "
                sqlSeoulRealTrade += " CNTRCT_PRD_END='" + arrCNTRCT_PRD[1] + "', "
                sqlSeoulRealTrade += " NEW_RON_SECD='" + dictSeoulRealtyTradeDataMaster['NEW_RON_SECD'] + "', "
                sqlSeoulRealTrade += " CNTRCT_UPDT_RQEST_AT='" + dictSeoulRealtyTradeDataMaster['CNTRCT_UPDT_RQEST_AT'] + "', "
                sqlSeoulRealTrade += " BEFORE_GRNTY_AMOUNT='" + dictSeoulRealtyTradeDataMaster['BEFORE_GRNTY_AMOUNT'] + "', "
                sqlSeoulRealTrade += " BEFORE_MT_RENT_CHRGE='" + dictSeoulRealtyTradeDataMaster['BEFORE_MT_RENT_CHRGE'] + "' "

                print("sqlSeoulRealTrade > ", sqlSeoulRealTrade)
                cursorRealEstate.execute(sqlSeoulRealTrade)
                nInsertedCount = nInsertedCount + 1

                ResRealEstateConnection.commit()

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = nLoop
                dictSwitchData['data_2'] = nInsertedCount
                dictSwitchData['data_3'] = strUniqueKey
                LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)




            #for list in jsonRowDatas:
            print("dictSeoulRealtyTradeDataMaster", "====================================================")
            nStartNumber = nEndNumber + 1
            nEndNumber = nEndNumber + nProcessedCount
            ResRealEstateConnection.close()

        #while True:
        print(GetLogDef.lineno(__file__), "End While")


    # For nProcessYear:
    print(GetLogDef.lineno(__file__), "End nProcessYears")

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
    print(GetLogDef.lineno(__file__), "dictSeoulSwitch >", dictSeoulSwitch)
    bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulUpdate(dictSeoulSwitch)
    print(GetLogDef.lineno(__file__), "bSwitchUpdateResult >", bSwitchUpdateResult)

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
    print('Inserted => ', nInsertedCount)
    print("========================= SUCCESS END")
finally:
    print("Finally END")



