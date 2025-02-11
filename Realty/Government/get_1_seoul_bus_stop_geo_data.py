# 지역 실거래 데이터 - 서울(41) 버스 정류장 위도경도 데이터 (75)
# 2025-02-10 커밋
#https://data.seoul.go.kr/dataList/OA-12912/S/1/datasetView.do


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


from Init.Functions.Logs import GetLogDef


from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Lib.Logging import UnifiedLogDeclarationFunction as ULF
from Lib.CustomException.QuitException import QuitException

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV

import xml.etree.ElementTree as ET


def XMLGetURL(url):


    while True:
        time.sleep(1)

        try:

            response = requests.get(url)
            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                         inspect.getframeinfo(inspect.currentframe()).lineno), "response===> ",
            #       type(response), response)
            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                         inspect.getframeinfo(inspect.currentframe()).lineno),
            #       "response.status_code===> ", type(response.status_code), response.status_code)
            if response.status_code == int(200):
                # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                #                         inspect.getframeinfo(inspect.currentframe()).lineno), "break ",
                #       type(response.raise_for_status()), response.raise_for_status())
                responseContents = response.text  # page_source 얻기
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                        inspect.getframeinfo(
                                            inspect.currentframe()).lineno) + "responseContents===> ",
                      type(responseContents), responseContents)
                ElementResponseRoot = ET.fromstring(responseContents)
                print("ElementResponseRoot===> ", type(ElementResponseRoot),  ElementResponseRoot, )

                strHeaderResultCode = ElementResponseRoot.find('RESULT').find('CODE').text
                strHeaderResultMessage = ElementResponseRoot.find('RESULT').find('MESSAGE').text
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                        inspect.getframeinfo(
                                            inspect.currentframe()).lineno) + "strHeaderResultCode===> ",
                      type(strHeaderResultCode), strHeaderResultCode)
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                        inspect.getframeinfo(
                                            inspect.currentframe()).lineno) + "strHeaderResultMessage===> ",
                      type(strHeaderResultMessage), strHeaderResultMessage)
                if strHeaderResultCode == 'INFO-000':
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(
                                                inspect.currentframe()).lineno) + "url===> ", type(url),
                          url)
                    return ElementResponseRoot

        except requests.exceptions.Timeout as e:
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(
                                        inspect.currentframe()).lineno) + "requests.exceptions.Timeout  url===> ",
                  type(url), url)
            time.sleep(10)

        except requests.exceptions.ConnectionError as errc:
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(
                                        inspect.currentframe()).lineno) + "requests.exceptions.ConnectionError  url===> ",
                  type(url), url)
            time.sleep(10)
        except requests.exceptions.HTTPError as errb:
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(
                                        inspect.currentframe()).lineno) + "requests.exceptions.HTTPError  url===> ",
                  type(url), url)
            time.sleep(10)

        # Any Error except upper exception
        except requests.exceptions.RequestException as erra:
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(
                                        inspect.currentframe()).lineno) + "requests.exceptions.RequestException  url===> ",
                  type(url), url)
            time.sleep(10)

        except Exception as e:

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(
                                        inspect.currentframe()).lineno) + "requests.exceptions.Exception  url===> ",
                  type(url), url)
            time.sleep(10)


def main():


    try:

        print("====================== TRY START")




        strProcessType = '034175'
        data_1 = '00'
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
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "[CRONTAB START]=============================")


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '20':
            strAddressSiguSequence = str(rstResult.get('data_1'))


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        nCallStartNumber = 1
        nCallProcessCount = 1000


        #DB 연결 선언
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        intNowProcessCount = 0

        while True:

            nCallEndNumber = int(nCallStartNumber) + int(nCallProcessCount) - 1

            url = "http://openapi.seoul.go.kr:8088/565a42515272656435326c484d6e64/xml/busStopLocationXyInfo/" + str(nCallStartNumber) + "/" + str(nCallEndNumber)
            print(GetLogDef.lineno(__file__), "url > ", url)

            ElementResponseRoot = XMLGetURL(url)

            if type(ElementResponseRoot) is not ET.Element:
                print(GetLogDef.lineno(__file__), "ElementResponseRoot 11111> ", type(ElementResponseRoot), ElementResponseRoot)
                raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'ElementResponseRoot => ' + str(ElementResponseRoot))  # 예외를 발생시킴

            print(GetLogDef.lineno(__file__), "ElementResponseRoot > ", type(ElementResponseRoot), ElementResponseRoot)

            # objectBodyItemAll = ElementResponseRoot.find('busStopLocationXyInfo').find('row')

            strListTotalCount = ElementResponseRoot.find('list_total_count').text

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strListTotalCount >> ",
                 strListTotalCount)

            intListTotalCount = int(strListTotalCount)

            objectBodyItemAll = ElementResponseRoot.findall('row')

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "objectBodyItemAll >> ",
                  len(objectBodyItemAll))
            # # intGetCount = len(objectBodyItemAll)


            intNowProcessCount = 0
            intInsertProcessCount = 0
            intUpdateProcessCount = 0
            for objectBodyItem in objectBodyItemAll:

                STOPS_ARS_NO = objectBodyItem.find('STOPS_NO').text
                STOPS_ID = objectBodyItem.find('NODE_ID').text
                SBWY_STNS_NM = objectBodyItem.find('STOPS_NM').text

                # <XCRD>127.0338672615</XCRD>lng
                # <YCRD>37.5578953118</YCRD>lat

                lat = objectBodyItem.find('YCRD').text #위도 lat
                lng = objectBodyItem.find('XCRD').text #경도 lng
                STOPS_TYPE = objectBodyItem.find('STOPS_TYPE').text

                sqlSelectBusStopGeodata  = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable + "  "
                sqlSelectBusStopGeodata += " WHERE STOPS_ID = %s"
                cursorRealEstate.execute(sqlSelectBusStopGeodata, (STOPS_ID))
                intSelectedCount = cursorRealEstate.rowcount

                if intSelectedCount < 1:

                    listInsertValue = []
                    listInsertValue.append(STOPS_ARS_NO)
                    listInsertValue.append(STOPS_ID)
                    listInsertValue.append(SBWY_STNS_NM)
                    listInsertValue.append(lat)
                    listInsertValue.append(lng)
                    listInsertValue.append(STOPS_TYPE)

                    sqlInsertBusStopGeodata = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable + " SET "
                    sqlInsertBusStopGeodata += " STOPS_ARS_NO = %s"
                    sqlInsertBusStopGeodata += " , STOPS_ID = %s"
                    sqlInsertBusStopGeodata += " , SBWY_STNS_NM = %s"
                    sqlInsertBusStopGeodata += " , lat = %s"
                    sqlInsertBusStopGeodata += " , lng = %s"
                    sqlInsertBusStopGeodata += " , STOPS_TYPE = %s"
                    sqlInsertBusStopGeodata += " , geo_point = ST_GeomFromText('POINT(" + lng + " " + lat + ")', 4326, 'axis-order=long-lat') "
                    cursorRealEstate.execute(sqlInsertBusStopGeodata, listInsertValue)
                    strSequence = cursorRealEstate.lastrowid
                    intInsertProcessCount += 1

                    print("INSERT", strSequence)

                else:

                    rstSeoulBusGeoData = cursorRealEstate.fetchone()
                    strSequence = str(rstSeoulBusGeoData.get('seq'))

                    print("UPDATE", strSequence)

                    strDBSTOPS_ARS_NO = str(rstSeoulBusGeoData.get('STOPS_ARS_NO'))
                    strDBSBWY_STNS_NM = str(rstSeoulBusGeoData.get('SBWY_STNS_NM'))

                    bChangeInfo = False

                    if strDBSTOPS_ARS_NO != STOPS_ARS_NO:
                        bChangeInfo = True
                    if strDBSBWY_STNS_NM != SBWY_STNS_NM:
                        bChangeInfo = True


                    sqlUpdateBusStopGeodata = " UPDATE " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable + " SET "
                    sqlUpdateBusStopGeodata += " modify_date = NOW()"
                    sqlUpdateBusStopGeodata += " , STOPS_ARS_NO = %s"
                    sqlUpdateBusStopGeodata += " , SBWY_STNS_NM = %s"

                    if bChangeInfo == True:
                        sqlUpdateBusStopGeodata += " ,state = '00' "

                    sqlUpdateBusStopGeodata += " WHERE seq = %s"
                    cursorRealEstate.execute(sqlUpdateBusStopGeodata,
                                             (STOPS_ARS_NO, SBWY_STNS_NM, strSequence))
                    intUpdateProcessCount += 1






                ResRealEstateConnection.commit()
                intNowProcessCount += 1

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = nCallStartNumber
            dictSwitchData['data_2'] = nCallEndNumber
            dictSwitchData['data_3'] = intInsertProcessCount
            dictSwitchData['data_4'] = intUpdateProcessCount
            dictSwitchData['data_5'] = STOPS_ID
            dictSwitchData['data_6'] = SBWY_STNS_NM
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            if intNowProcessCount < nCallProcessCount:
                print("intNowProcessCount => ", intNowProcessCount)
                break

            nCallStartNumber += nCallProcessCount
            time.sleep(3)





        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)




    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
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
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
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
        print("========================= SUCCESS END")

    finally:
        print("Finally END")
        ResRealEstateConnection.close()




if __name__ == '__main__':
    main()
