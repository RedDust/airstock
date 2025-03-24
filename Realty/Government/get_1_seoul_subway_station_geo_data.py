# 지역 실거래 데이터 - 서울(41) 버스 정류장 위도경도 데이터 (75)
# 2025-02-10 커밋
#https://data.seoul.go.kr/dataList/OA-12912/S/1/datasetView.do
#"서울시 OPEN API 샘플URL
#"샘플URL	http://t-data.seoul.go.kr/apig/apiman-gateway/tapi/TaimsKsccDvSubwayStationGeom/1.0"
#"샘플 미리보기	{ "outStnNum": "4128", "stnKrNm": "삼성중앙", "lineNm": "9호선(연장)", "convX": "127.053282", "convY": "37.513011" }"


import sys
import json
import time
import random
import pymysql
import inspect
import requests
import traceback
import re

sys.path.append("D:/PythonProjects/airstock")


import urllib.request
import json
import pymysql
import datetime, time, inspect


from Lib.RDB import pyMysqlConnector


from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Realty.Government.Lib.Logging import UnifiedLogDeclarationFunction as ULF
from Lib.CustomException.QuitException import QuitException

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from dateutil.relativedelta import relativedelta


def GetLastData(cursorRealEstate):

    sqlSelectLastDay = "SELECT * FROM kt_realty_seoul_subway_master ORDER BY USE_DT DESC LIMIT 1"
    cursorRealEstate.execute(sqlSelectLastDay)
    intSelectedCount = cursorRealEstate.rowcount
    rstSeoulBusGeoData = cursorRealEstate.fetchone()
    print("sqlSelectLastDay ==>  => ", sqlSelectLastDay)
    print("intSelectedCount => ", intSelectedCount)

    print("rstSeoulBusGeoData => ", rstSeoulBusGeoData)
    strDBUSE_YMD = rstSeoulBusGeoData.get('USE_DT')
    return strDBUSE_YMD




def GetSubwayGeo_1():
# 1. 지하철역 좌표 정보 수집

    try:
        print("====================== GetSubwayGeo_1 START")

        strProcessType = '034181'
        data_1 = '1'
        data_2 = '00'
        data_3 = '00'
        data_4 = '00'
        data_5 = '00'
        data_6 = '00'

        dtNow = DateTime.today()
        LogPath = 'Government/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        #
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "[GetSubwayGeo_1 START]=============================")


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '20':
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
        url = "https://t-data.seoul.go.kr/apig/apiman-gateway/tapi/TaimsKsccDvSubwayStationGeom/1.0?apikey="+ init_conf.T_SeoulAuthorizationKey
        # url += "&pageNo=" + str(nStartNumber) + "&numOfRows=1&type=json"

        # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
        response = urllib.request.urlopen(url)

        # print("response => " , response)


        if response.getcode() != 200:
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[response >>" + str(response) + "]")
            json_str = response.read().decode("utf-8")
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[json_str >>" + str(json_str) + "]")
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[response.getcode() >>" + str(response.getcode()) + "]")

        json_str = response.read().decode("utf-8")

        # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
        jsonResponseDatas = json.loads(json_str)

        # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "jsonResponseDatas > ", jsonResponseDatas)
        intProcessCount = 0
        intInsertProcessCount = 0
        intUpdateProcessCount = 0
        for jsonResponseData in jsonResponseDatas:
            intProcessCount += 1
            data_4 = str(intProcessCount)
            strOutStnNum = str(jsonResponseData['outStnNum'])
            data_1 = strOutStnNum
            strStnKrNm = str(jsonResponseData['stnKrNm'])
            data_2 = strStnKrNm
            strLineNm = str(jsonResponseData['lineNm'])
            data_3 = strLineNm
            strLng = str(jsonResponseData['convX'])
            strLat = str(jsonResponseData['convY'])

            sqlSelectSubwayGeo = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable
            sqlSelectSubwayGeo += " WHERE STOPS_ID=%s "
            cursorRealEstate.execute(sqlSelectSubwayGeo,(strOutStnNum))
            intSelectedCount = cursorRealEstate.rowcount

            if intSelectedCount < 1:
                intInsertProcessCount += 1
                data_5 = str(intInsertProcessCount)
                sqlSelectSubwayGeo = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable + " SET "
                sqlSelectSubwayGeo += " STOPS_ID = %s "
                sqlSelectSubwayGeo += " , SBWY_STNS_NM = %s "
                sqlSelectSubwayGeo += " , LINE_NM = %s "
                sqlSelectSubwayGeo += " , lat = %s "
                sqlSelectSubwayGeo += " , lng = %s "
                sqlSelectSubwayGeo += " , geo_point = ST_GeomFromText('POINT(" + strLng + " " + strLat + ")', 4326, 'axis-order=long-lat') "
                print("sqlSelectSubwayGeo" , sqlSelectSubwayGeo)

                cursorRealEstate.execute(sqlSelectSubwayGeo, (strOutStnNum, strStnKrNm, strLineNm, strLat, strLng))


            else:
                intUpdateProcessCount += 1
                data_6 = str(intUpdateProcessCount)
                sqlSelectSubwayGeo = " UPDATE " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable + " SET "
                sqlSelectSubwayGeo += " modify_date=NOW() "
                sqlSelectSubwayGeo += " , SBWY_STNS_NM=%s "
                sqlSelectSubwayGeo += " , LINE_NM=%s "
                sqlSelectSubwayGeo += " WHERE STOPS_ID=%s "
                cursorRealEstate.execute(sqlSelectSubwayGeo,(strStnKrNm, strLineNm, strOutStnNum))

            ResRealEstateConnection.commit()


            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = data_1
            dictSwitchData['data_2'] = data_2
            dictSwitchData['data_3'] = data_3
            dictSwitchData['data_4'] = data_4
            dictSwitchData['data_5'] = data_5
            dictSwitchData['data_6'] = data_6
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayGeo_1 Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayGeo_1 Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        print("========================= GetSubwayGeo_1 SUCCESS END")

    finally:
        print("Finally END")
        ResRealEstateConnection.close()



def GetSubwayAddress_2():
# 2. 지하철역 좌표 별 주소 수집

    try:
        print("====================== GetSubwayAddress_2 START")

        strProcessType = '034181'
        data_1 = '2'
        data_2 = '00'
        data_3 = '00'
        data_4 = '00'
        data_5 = '00'
        data_6 = '00'

        dtNow = DateTime.today()
        LogPath = 'Government/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        #
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[GetSubwayAddress_2 START]=============================")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '20':
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        sqlSelectSubwayGeoTable = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable
        sqlSelectSubwayGeoTable += " WHERE state='00' "
        # sqlSelectSubwayGeoTable += " limit 5 "
        cursorRealEstate.execute(sqlSelectSubwayGeoTable)
        intSelectedCount = cursorRealEstate.rowcount

        intProcessCount = 0
        intUpdateProcessCount = 0

        if intSelectedCount > 0:

            rstSeoulSubwayGeoDatas = cursorRealEstate.fetchall()

            for rstSeoulSubwayGeoData in rstSeoulSubwayGeoDatas:
                strSequence = str(rstSeoulSubwayGeoData.get('seq'))
                data_1 = strSequence

                strNMMTLat = str(rstSeoulSubwayGeoData.get('lat'))
                strNMMTLng = str(rstSeoulSubwayGeoData.get('lng'))
                strDBSBWY_STNS_NM = str(rstSeoulSubwayGeoData.get('SBWY_STNS_NM'))
                data_3 = strDBSBWY_STNS_NM

                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strDBSBWY_STNS_NM => ", strDBSBWY_STNS_NM)

                if intUpdateProcessCount % 6 == 5:
                    time.sleep(1)

                dictReverseGeoData = GeoDataModule.getNaverReverseGeoData(logging, str(strNMMTLng), str(strNMMTLat))
                if dictReverseGeoData == False:
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "dictReverseGeoData False]")
                    continue

                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "dictReverseGeoData => ",
                      dictReverseGeoData)

                strAddressCode = str(dictReverseGeoData['dosi_code'])
                data_2 =strAddressCode

                strDosiCode = str(strAddressCode)[:2]
                strSigunCode = str(strAddressCode)[2:5]
                strDongMyunCode = str(strAddressCode)[5:]
                strDosiName = str(dictReverseGeoData['dosi_name'])
                strSigunName = str(dictReverseGeoData['sigu_name'])
                data_5 = strSigunName
                strDongMyunName = str(dictReverseGeoData['dong_name'])
                data_6 = strDongMyunName
                BONBEON = str(dictReverseGeoData['bone_bun']).zfill(4)
                BUBEON = str(dictReverseGeoData['bone_bun']).zfill(4)

                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strDosiCode => ", strDosiCode)
                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strSigunCode => ", strSigunCode)
                print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strDongMyunCode => ", strDongMyunCode)

                listUpdateData=[]
                listUpdateData.append(strDosiCode)
                listUpdateData.append(strDosiName)
                listUpdateData.append(strSigunCode)
                listUpdateData.append(strSigunName)
                listUpdateData.append(strDongMyunCode)
                listUpdateData.append(strDongMyunName)
                listUpdateData.append(BONBEON)
                listUpdateData.append(BUBEON)
                listUpdateData.append(strAddressCode)
                listUpdateData.append(strSequence)

                sqlUpdateBusStopGeodata = " UPDATE " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable + " SET "
                sqlUpdateBusStopGeodata += " modify_date = NOW()"
                sqlUpdateBusStopGeodata += " , state = '01' "
                sqlUpdateBusStopGeodata += " , SIDO_CD = %s "
                sqlUpdateBusStopGeodata += " , SIDO_NM = %s "
                sqlUpdateBusStopGeodata += " , SGG_CD = %s "
                sqlUpdateBusStopGeodata += " , SGG_NM = %s "
                sqlUpdateBusStopGeodata += " , BJDONG_CD = %s "
                sqlUpdateBusStopGeodata += " , BJDONG_NM = %s "
                sqlUpdateBusStopGeodata += " , BONBEON = %s "
                sqlUpdateBusStopGeodata += " , BUBEON = %s "
                sqlUpdateBusStopGeodata += " , ADDRESS_CODE = %s "
                sqlUpdateBusStopGeodata += " WHERE seq = %s"
                cursorRealEstate.execute(sqlUpdateBusStopGeodata, listUpdateData)
                ResRealEstateConnection.commit()
                intUpdateProcessCount += 1
                data_4 = intUpdateProcessCount


                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = data_1
                dictSwitchData['data_2'] = data_2
                dictSwitchData['data_3'] = data_3
                dictSwitchData['data_4'] = data_4
                dictSwitchData['data_5'] = data_5
                dictSwitchData['data_6'] = data_6
                LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


            intProcessCount += 1

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayAddress_2 Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayAddress_2 Error QuitException]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        print("========================= GetSubwayAddress_2 SUCCESS END")

    finally:
        print("GetSubwayAddress_2 Finally END")
        ResRealEstateConnection.close()






def GetSubwayNameInfo_3():
# 3. 지하철역 이름 수집 (중복 데이터 지만 제공처가 다름)

    try:
        print("====================== GetSubwayRidingCount_3 START")

        strProcessType = '034181'
        data_1 = '3'
        data_2 = '00'
        data_3 = '00'
        data_4 = '00'
        data_5 = '00'
        data_6 = '00'

        dtNow = DateTime.today()
        LogPath = 'Government/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        #
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[GetSubwayRidingCount_3 START]=============================")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        # if strResult == '20':
        #     raise Exception(
        #         SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        nStartNumber = 1
        nEndNumber = 1000

        # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
        url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey
        url += "/json/SearchSTNBySubwayLineInfo/" + str(nStartNumber) + "/" + str(nEndNumber)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'url => ' + str(url))  # 예외를 발생시킴

        # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
        response = urllib.request.urlopen(url)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[response : (" + str(response) + ")")

        json_str = response.read().decode("utf-8")

        # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
        json_object = json.loads(json_str)
        bMore = json_object.get('SearchSTNBySubwayLineInfo')

        if bMore is None:
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'bMore => [None]')  # 예외를 발생시킴

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        nTotalCount = bMore.get('list_total_count')
        jsonResultDatas = bMore.get('RESULT')
        jsonResultDatasResult = jsonResultDatas

        strResultCode = jsonResultDatasResult.get('CODE')
        strResultMessage = jsonResultDatasResult.get('MESSAGE')

        if strResultCode != 'INFO-000':
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResultCode => [' + str(
                    strResultCode) + ']')  # 예외를 발생시킴
            # GetOut while True:

        jsonRowDatas = bMore.get('row')

        # 3. 건별 처리
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[Processing]######################################################")

        for jsonRowData in jsonRowDatas:

            STATION_CD = str(jsonRowData.get('STATION_CD'))
            data_2 =STATION_CD
            STATION_NM = str(jsonRowData.get('STATION_NM'))
            data_3 = STATION_NM
            LINE_NUM = str(jsonRowData.get('LINE_NUM'))
            data_4 = LINE_NUM

            sqlSelectSubwayGeoTable = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable
            sqlSelectSubwayGeoTable += " WHERE STOPS_ID= %s LIMIT 1"
            cursorRealEstate.execute(sqlSelectSubwayGeoTable, (STATION_CD))
            intSelectedCount = cursorRealEstate.rowcount

            if intSelectedCount < 1:
                print("STATION_CD => INSERT ")
                print("STATION_NM => ", STATION_NM)
                print("LINE_NUM => ", LINE_NUM)
                sqlInsertSubwayGeoTable = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable + " SET "
                sqlInsertSubwayGeoTable += " STATION_CODE = %s  "
                sqlInsertSubwayGeoTable += " , STATION_NAME = %s  "
                sqlInsertSubwayGeoTable += " , LINE_NUMBER = %s  "
                sqlInsertSubwayGeoTable += " , geo_point = ST_GeomFromText('POINT( 0 0 )', 4326, 'axis-order=long-lat') "
                sqlInsertSubwayGeoTable += " , state = '03'  "
                cursorRealEstate.execute(sqlInsertSubwayGeoTable, (STATION_CD,STATION_NM,LINE_NUM ))


            else:
                # print("STATION_CD => UPDATE ")
                # print("STATION_NM => ", STATION_NM)
                # print("LINE_NUM => ", LINE_NUM)

                rstSelectData = cursorRealEstate.fetchone()

                # print("rstSelectData => ", rstSelectData)

                strSequence = str(rstSelectData.get('seq'))

                sqlInsertSubwayGeoTable = " UPDATE " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable + " SET "
                sqlInsertSubwayGeoTable += " STATION_CODE = %s  "
                sqlInsertSubwayGeoTable += " , STATION_NAME = %s  "
                sqlInsertSubwayGeoTable += " , LINE_NUMBER = %s  "
                sqlInsertSubwayGeoTable += " , state = '03'  "
                sqlInsertSubwayGeoTable += " WHERE seq = %s  "
                cursorRealEstate.execute(sqlInsertSubwayGeoTable, (STATION_CD, STATION_NM, LINE_NUM, strSequence))


            ResRealEstateConnection.commit()


            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = "3"
            dictSwitchData['data_2'] = data_2
            dictSwitchData['data_3'] = data_3
            dictSwitchData['data_4'] = data_4
            dictSwitchData['data_5'] = data_5
            dictSwitchData['data_6'] = data_6
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


            # intProcessCount += 1

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayRidingCount_3 Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayRidingCount_3 Error QuitException]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        print("========================= GetSubwayRidingCount_3 SUCCESS END")

    finally:
        print("GetSubwayAddress_2 Finally END")
        ResRealEstateConnection.close()


def GetSubwayRidingCount_4():
# 탑승 역, 호선별로  탑습인원을 좌표 테이블에 저장
# 2호선 잠실역 XXXX 명
# 8호선 잠실역 XXXX 명

    try:
        print("====================== GetSubwayRidingCount_3 START")

        strProcessType = '034181'
        data_1 = '4'
        data_2 = '00'
        data_3 = '00'
        data_4 = '00'
        data_5 = '00'
        data_6 = '00'

        dtNow = DateTime.today()
        LogPath = 'Government/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        #
        logging.info(SLog.Ins(Isp.getframeinfo,
                              Isp.currentframe()) + "[GetSubwayRidingCount_3 START]=============================")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        # if strResult == '20':
        #     raise Exception(
        #         SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        # DB 연결 선언
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        strUseDT = str(GetLastData(cursorRealEstate))[:6]
        print("strUseDT => ", strUseDT)


        sqlSelectSubwayMaster = " SELECT SUB_STA_NM,LINE_NUM, SUM(RIDE_PASGR_NUM) AS RIDE_SUM_NUM ,SUM(ALIGHT_PASGR_NUM) AS ALIGHT_SUM_NUM "

        sqlSelectSubwayMaster += " FROM " + ConstRealEstateTable_GOV.SeoulSubwayDataTable
        sqlSelectSubwayMaster += " WHERE USE_DT LIKE '"+strUseDT+"%' "
        sqlSelectSubwayMaster += " GROUP BY SUB_STA_NM,LINE_NUM "
        # sqlSelectSubwayMaster += " LIMIT 10 "

        cursorRealEstate.execute(sqlSelectSubwayMaster)
        rstSelectDatas = cursorRealEstate.fetchall()

        for rstSelectData in rstSelectDatas:

            # print("rstSelectData =>", rstSelectData)

            strMasterLINE_NUM = str(rstSelectData.get('LINE_NUM'))
            strMasterSUB_STA_NM = str(rstSelectData.get('SUB_STA_NM'))

            strRIDE_PASGR_NUM = str(rstSelectData.get('RIDE_SUM_NUM'))
            strALIGHT_PASGR_NUM = str(rstSelectData.get('ALIGHT_SUM_NUM'))


            sqlSelectGeoTable = " SELECT * FROM  " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable
            sqlSelectGeoTable += " WHERE (SBWY_STNS_NM = %s OR STATION_NAME =%s )"
            sqlSelectGeoTable += " AND (LINE_NM = %s OR LINE_NUMBER =%s )"

            # print("sqlSelectGeoTable =>", sqlSelectGeoTable)

            cursorRealEstate.execute(sqlSelectGeoTable, (strMasterSUB_STA_NM,strMasterSUB_STA_NM,strMasterLINE_NUM,strMasterLINE_NUM))
            # rstMasterDatas = cursorRealEstate.fetchall()

            intSelectedCount = cursorRealEstate.rowcount
            rstMasterDatas = cursorRealEstate.fetchall()
            if intSelectedCount != 1:
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Exception strMasterSUB_STA_NM : (" + str(strMasterSUB_STA_NM) + ")")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Exception strMasterSUB_STA_NM : (" + str(strMasterSUB_STA_NM) + ")")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Exception strMasterLINE_NUM : (" + str(strMasterLINE_NUM) + ")")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Exception strMasterLINE_NUM : (" + str(strMasterLINE_NUM) + ")")
                raise Exception(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'intSelectedCount => ' + str(intSelectedCount))  # 예외를 발생시킴

            for rstMasterData in rstMasterDatas:
                # print("rstSelectData ============================>", rstMasterData)
                print("rstMasterData =>", rstMasterDatas, strMasterSUB_STA_NM, strMasterSUB_STA_NM,
                      strMasterLINE_NUM, strMasterLINE_NUM , strRIDE_PASGR_NUM, strALIGHT_PASGR_NUM)
                strGeoTableSequence = str(rstMasterData.get('seq'))

                sqlUpdateGeoTable = " UPDATE " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable + " SET "
                sqlUpdateGeoTable += " GTON_TNOPE = %s "
                sqlUpdateGeoTable += " ,GTOFF_TNOPE = %s "
                sqlUpdateGeoTable += " WHERE seq = %s "
                cursorRealEstate.execute(sqlUpdateGeoTable, (strRIDE_PASGR_NUM, strALIGHT_PASGR_NUM, strGeoTableSequence))

                ResRealEstateConnection.commit()

                print("rstSelectData ELSE ------------------------------>", rstMasterDatas)


            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = "4"
            dictSwitchData['data_2'] = data_2
            dictSwitchData['data_3'] = data_3
            dictSwitchData['data_4'] = data_4
            dictSwitchData['data_5'] = data_5
            dictSwitchData['data_6'] = data_6
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            # intProcessCount += 1

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayRidingCount_3 Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayRidingCount_3 Error QuitException]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        print("========================= GetSubwayRidingCount_3 SUCCESS END")

    finally:
        print("GetSubwayAddress_2 Finally END")


def GetSubwayMasterRidingCount_5():
    # 탑승 역, 호선별로  탑습인원을 좌표 테이블에 저장
    # 잠실역 XXXX (2호선 + 8호선) 명
    try:
        print("====================== GetSubwayRidingCount_3 START")

        strProcessType = '034181'
        data_1 = '3'
        data_2 = '00'
        data_3 = '00'
        data_4 = '00'
        data_5 = '00'
        data_6 = '00'

        dtNow = DateTime.today()
        LogPath = 'Government/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        #
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[GetSubwayRidingCount_3 START]=============================")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        # if strResult == '20':
        #     raise Exception(
        #         SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        # DB 연결 선언
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        sqlUpdateGeoTable = "UPDATE "+ ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable + " SET "
        sqlUpdateGeoTable += " MASTER_GTON_TNOPE = 0 "
        sqlUpdateGeoTable += " , MASTER_GTOFF_TNOPE = 0 "
        sqlUpdateGeoTable += " , master_flag = '00' "
        sqlUpdateGeoTable += " WHERE state!='99' "
        cursorRealEstate.execute(sqlUpdateGeoTable)


        sqlSelectGeoTable = " SELECT SBWY_STNS_NM , min(seq) as SEQUENCE "
        sqlSelectGeoTable += " , SUM(GTON_TNOPE) AS GTON_MASTER_TNOPE , SUM(GTOFF_TNOPE) AS GTOFF_MASTER_TNOPE "
        sqlSelectGeoTable += " FROM " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable
        sqlSelectGeoTable += " WHERE GTON_TNOPE > 0 "
        sqlSelectGeoTable += " GROUP BY SBWY_STNS_NM "
        sqlSelectGeoTable += " ORDER BY  GTON_MASTER_TNOPE DESC"
        # sqlSelectGeoTable += " LIMIT 10 "


        cursorRealEstate.execute(sqlSelectGeoTable)
        intSelectedCount = cursorRealEstate.rowcount
        if intSelectedCount < 1:
            raise Exception(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'intSelectedCount => ' + str(intSelectedCount))  # 예외를 발생시킴

        rstGeoTableDatas = cursorRealEstate.fetchall()

        for rstGeoTableData in rstGeoTableDatas:
            print("rstGeoTableData =>" , rstGeoTableData)
            strGeoSequence = str(rstGeoTableData.get('SEQUENCE'))
            strGeoSBWY_STNS_NM = str(rstGeoTableData.get('SBWY_STNS_NM'))
            strGeoSBWY_GTON_MASTER_TNOPE= str(rstGeoTableData.get('GTON_MASTER_TNOPE'))
            strGeoSBWY_GTOFF_MASTER_TNOPE = str(rstGeoTableData.get('GTOFF_MASTER_TNOPE'))

            sqlSelectGeoStation = " UPDATE " + ConstRealEstateTable_GOV.SeoulSubwayGeoDataTable + " SET "
            sqlSelectGeoStation += " modify_date = NOW() "
            sqlSelectGeoStation += " , master_flag = '10' "
            sqlSelectGeoStation += " , MASTER_GTON_TNOPE = %s "
            sqlSelectGeoStation += " , MASTER_GTOFF_TNOPE = %s "
            sqlSelectGeoStation += " WHERE seq = %s "
            print("sqlSelectGeoStation =>", sqlSelectGeoStation, strGeoSequence)
            cursorRealEstate.execute(sqlSelectGeoStation, (strGeoSBWY_GTON_MASTER_TNOPE,strGeoSBWY_GTOFF_MASTER_TNOPE,  strGeoSequence))

            ResRealEstateConnection.commit()


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = "4"
        dictSwitchData['data_2'] = data_2
        dictSwitchData['data_3'] = data_3
        dictSwitchData['data_4'] = data_4
        dictSwitchData['data_5'] = data_5
        dictSwitchData['data_6'] = data_6
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            # intProcessCount += 1

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayRidingCount_3 Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[GetSubwayRidingCount_3 Error QuitException]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        print("========================= GetSubwayRidingCount_3 SUCCESS END")

    finally:
        print("GetSubwayAddress_2 Finally END")



def main():
    # GetSubwayGeo_1()
    # GetSubwayAddress_2()
    # GetSubwayNameInfo_3()
    GetSubwayRidingCount_4()
    GetSubwayMasterRidingCount_5()

if __name__ == '__main__':
    main()
