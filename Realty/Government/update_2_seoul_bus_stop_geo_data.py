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


from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Lib.Logging import UnifiedLogDeclarationFunction as ULF
from Lib.CustomException.QuitException import QuitException

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV

def main():


    try:
        print("====================== TRY START")

        strProcessType = '034176'
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


        sqlSelectBusStopGeodata  = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable + "  "
        sqlSelectBusStopGeodata += " WHERE state = '00' "
        # sqlSelectBusStopGeodata += " LIMIT 100 "
        cursorRealEstate.execute(sqlSelectBusStopGeodata)
        data_3 = strSelectedCount = str(cursorRealEstate.rowcount)

        rstSeoulBusGeoDatas = cursorRealEstate.fetchall()
        intUpdateProcessCount = 0
        strSequence='0'
        data_1 = strSequence
        for rstSeoulBusGeoData in rstSeoulBusGeoDatas:

            strSequence = str(rstSeoulBusGeoData.get('seq'))
            data_1 =strSequence
            strLatitude = str(rstSeoulBusGeoData.get('lat'))
            strLongtitude = str(rstSeoulBusGeoData.get('lng'))
            floatNMMTLat = float(rstSeoulBusGeoData.get('lat'))
            floatNMMTLng = float(rstSeoulBusGeoData.get('lng'))
            strDBSBWY_STNS_NM = str(rstSeoulBusGeoData.get('SBWY_STNS_NM'))

            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "strDBSBWY_STNS_NM => ", strDBSBWY_STNS_NM)

            if intUpdateProcessCount % 6 == 5:
                time.sleep(1)

            dictReverseGeoData = GeoDataModule.getNaverReverseGeoData(logging, str(floatNMMTLng), str(floatNMMTLat))
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

            sqlUpdateBusStopGeodata = " UPDATE " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable + " SET "
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
            dictSwitchData['data_1'] = strSequence
            dictSwitchData['data_2'] = strAddressCode
            dictSwitchData['data_3'] = strSelectedCount
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
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if strAddressCode is not None:
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
