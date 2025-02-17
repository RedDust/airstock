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
from Realty.Government.Lib.Logging import UnifiedLogDeclarationFunction as ULF
from Lib.CustomException.QuitException import QuitException

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from dateutil.relativedelta import relativedelta

def GetLastData(cursorRealEstate, dtNow):

    strInsertYear = str(dtNow.year)

    while True:

        print("strInsertYear => ", strInsertYear)

        sqlSelectLastDay = "SELECT * FROM kt_realty_seoul_bus_using_static_" + strInsertYear + " ORDER BY USE_YMD DESC LIMIT 1"
        cursorRealEstate.execute(sqlSelectLastDay)
        intSelectedCount = cursorRealEstate.rowcount
        rstSeoulBusGeoData = cursorRealEstate.fetchone()
        print("sqlSelectLastDay ==>  => ", sqlSelectLastDay)
        print("intSelectedCount => ", intSelectedCount)

        if intSelectedCount > 0:
            break

        dtNow = dtNow - relativedelta(years=1)
        strInsertYear = str(dtNow.year)

    print("rstSeoulBusGeoData => ", rstSeoulBusGeoData)
    strDBUSE_YMD = rstSeoulBusGeoData.get('USE_YMD')
    return strDBUSE_YMD



def getMasterID(cursorRealEstate, strDBGeo_STOPS_ID):
    strInputGeo_STOPS_ID = strDBGeo_STOPS_ID

    while True:

        print("6.getMasterID->", strDBGeo_STOPS_ID)
        sqlSlaveForSlave = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable + " "
        sqlSlaveForSlave += " WHERE  STOPS_ID = %s "
        cursorRealEstate.execute(sqlSlaveForSlave, (strDBGeo_STOPS_ID))
        rstSelectMasterGeoDataForUpdate = cursorRealEstate.fetchone()
        strSelectMasterFlag = rstSelectMasterGeoDataForUpdate.get('master_flag')
        strDBGeo_STOPS_ID = rstSelectMasterGeoDataForUpdate.get('MASTER_STOPS_ID')
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[getMasterID strSelectMasterFlag : " + str(
                strSelectMasterFlag))

        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDBGeo_STOPS_ID strDBGeo_STOPS_ID : " + str(
                strSelectMasterFlag))

        if strSelectMasterFlag == '00':
            strDBGeo_STOPS_ID = strInputGeo_STOPS_ID
            break
        elif strSelectMasterFlag == '10':
            break

    return strDBGeo_STOPS_ID


def UpdateMasterCount_3():

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
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "[UpdateMasterCount START]=============================")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '20':
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴


        # DB 연결 선언
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        print("0.전체 SELECT START  =>  ")
        sqlSelectMasterID = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable
        sqlSelectMasterID += " WHERE master_flag = '10'  "
        sqlSelectMasterID += " ORDER BY MASTER_GTON_TNOPE DESC "

        cursorRealEstate.execute(sqlSelectMasterID)
        rstSelectForGeoDatas = cursorRealEstate.fetchall()
        print("0.전체 SELECT END  =>  ")

        for rstSelectForGeoData in rstSelectForGeoDatas:
            strMasterSTOPS_ID = rstSelectForGeoData.get('STOPS_ID')
            data_1 = strMasterSTOPS_ID
            print("1.")
            print("1. rstSelectForGeoData ", strMasterSTOPS_ID, " ======================================================================>  ")
            print("1. rstSelectForGeoData  =>  ", rstSelectForGeoData)


            sqlSelectSlaveData = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable
            sqlSelectSlaveData += " WHERE MASTER_STOPS_ID = %s  "
            sqlSelectMasterID += " ORDER BY GTON_TNOPE DESC "
            cursorRealEstate.execute(sqlSelectSlaveData , (strMasterSTOPS_ID))
            rstSelectSlaveCounts = cursorRealEstate.fetchall()

            intMasterGTON_TNOPE =0
            intMasterGTOFF_TNOPE = 0
            strListSLAVE_STOPS_ID = strMasterSTOPS_ID+"|"
            for rstSelectSlaveCount in rstSelectSlaveCounts:
                strSlaveMasterSTOPS_ID = rstSelectSlaveCount.get('STOPS_ID')
                intSlaveGTON_TNOPE = int(rstSelectSlaveCount.get('GTON_TNOPE'))
                intSlaveGTOFF_TNOPE = int(rstSelectSlaveCount.get('GTOFF_TNOPE'))
                strListSLAVE_STOPS_ID += strSlaveMasterSTOPS_ID + ","

                intMasterGTON_TNOPE += intSlaveGTON_TNOPE
                intMasterGTOFF_TNOPE += intSlaveGTOFF_TNOPE

                print("2. rstSelectForGeoData ", strSlaveMasterSTOPS_ID, " +++++++++++++++++++++++++++  ")
                print("2. strSlaveMasterSTOPS_ID  =>  ", strSlaveMasterSTOPS_ID)
                print("2. intMasterGTON_TNOPE  =>  ", intMasterGTON_TNOPE)
                print("2. intMasterGTOFF_TNOPE  =>  ", intMasterGTOFF_TNOPE)
                print("2. rstSelectSlaveCount  =>  ", rstSelectSlaveCount)

            print("3. UPDATE ", strMasterSTOPS_ID, " --------------------------  ")
            strMasterGTON_TNOPE = str(intMasterGTON_TNOPE)
            strMasterGTOFF_TNOPE = str(intMasterGTOFF_TNOPE)

            print("3. strMasterGTON_TNOPE ", strMasterGTON_TNOPE)
            print("3. strMasterGTOFF_TNOPE ", strMasterGTOFF_TNOPE)

            sqlUpdateMasterData = " UPDATE " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable
            sqlUpdateMasterData += " SET MASTER_GTON_TNOPE = %s  "
            sqlUpdateMasterData += " , MASTER_GTOFF_TNOPE = %s  "
            sqlUpdateMasterData += " ,SLAVE_STOPS_ID = %s "
            sqlUpdateMasterData += " WHERE STOPS_ID = %s  "
            cursorRealEstate.execute(sqlUpdateMasterData,
                                     (strMasterGTON_TNOPE, strMasterGTOFF_TNOPE, strListSLAVE_STOPS_ID, strMasterSTOPS_ID))
            ResRealEstateConnection.commit()

            data_2 = strListSLAVE_STOPS_ID
            data_3 = strMasterGTON_TNOPE
            data_4 = strMasterGTOFF_TNOPE

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

        print("4. 버스 정류장 근처 LOOP END ---->")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[QuitException QuitException]")
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


    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[QuitException QuitException]")
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
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "[UpdateMasterCount END]=============================")
        ResRealEstateConnection.close()



def UpdateMasterID_2():

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
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "[UpdateMasterID START]=============================")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        # if strResult == '10':
        #     raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴
        #
        # if strResult == '20':
        #     raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


        #DB 연결 선언
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        strDBUSE_YMD = GetLastData(cursorRealEstate, dtNow)

        print("1. 초기화  START =>  ", strDBUSE_YMD)

        sqlUpdateDebutBus = " UPDATE " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable + " SET "
        sqlUpdateDebutBus += " master_flag = '00' "
        sqlUpdateDebutBus += " ,MASTER_STOPS_ID = '' "
        sqlUpdateDebutBus += " ,MASTER_GTON_TNOPE = 0 "
        sqlUpdateDebutBus += " ,MASTER_GTOFF_TNOPE = 0 "
        cursorRealEstate.execute(sqlUpdateDebutBus)
        ResRealEstateConnection.commit()

        print("1. 초기화 END =>  ", strDBUSE_YMD)

        print("strDBUSE_YMD => ", strDBUSE_YMD)
        strDBUSE_YM = str(strDBUSE_YMD)[:6]
        strDBUSE_Y = str(strDBUSE_YMD)[:4]


        print("2.전체 SELECT START  =>  ", strDBUSE_YMD)
        sqlSelectForGeoData = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable
        # sqlSelectForGeoData += " WHERE BJDONG_CD = '10500' "
        sqlSelectForGeoData += " ORDER BY GTON_TNOPE DESC "
        # sqlSelectForGeoData += " LIMIT 5 "
        cursorRealEstate.execute(sqlSelectForGeoData)
        rstSelectForGeoDatas = cursorRealEstate.fetchall()
        print("2.전체 SELECT END  =>  ", strDBUSE_YMD)

        intMasterCount = 0
        intSlaveCount = 0

        for rstSelectForGeoData in rstSelectForGeoDatas:
            print("2. 버스 정류장 데이터 전체 LOOP =>  =======")
            strListSLAVE_STOPS_ID = ''
            logging.info("")
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[2. 버스 정류장 데이터 전체 LOOP : ==========================")
            strDBMasterTodayRank = str(rstSelectForGeoData.get('today_rank'))
            strDBGeoMasterSTOPS_ID = str(rstSelectForGeoData.get('STOPS_ID'))
            strDBMasterGeoLatitude = str(rstSelectForGeoData.get('lat'))
            strDBMasterGeoLongtitude = str(rstSelectForGeoData.get('lng'))
            strBusMasterSBWY_STNS_NM = str(rstSelectForGeoData.get('SBWY_STNS_NM'))
            strBusMasterSTOPS_TYPE = str(rstSelectForGeoData.get('STOPS_TYPE'))

            data_1 = strDBGeoMasterSTOPS_ID
            data_3 = strDBMasterTodayRank
            data_4 = strBusMasterSBWY_STNS_NM
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[2. 버스 정류장 데이터 전체 RANK : " + strDBMasterTodayRank )

            strListSLAVE_STOPS_ID += strDBGeoMasterSTOPS_ID + "|"

            print("3.근처 SELECT START  =>  ", strDBGeoMasterSTOPS_ID)
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[ 3. strDBGeoMasterSTOPS_ID : "+ str(strDBGeoMasterSTOPS_ID))

            sqlSelectSlaveGeoData = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable
            sqlSelectSlaveGeoData += " WHERE ST_Distance(geo_point, ST_GeomFromText(CONCAT('POINT(', %s, ' ', %s, ')'), 4326)) <= 120 "
            sqlSelectSlaveGeoData += " ORDER BY GTON_TNOPE DESC "
            cursorRealEstate.execute(sqlSelectSlaveGeoData, (strDBMasterGeoLatitude, strDBMasterGeoLongtitude))
            rstSeoulBusSlaveGeos = cursorRealEstate.fetchall()
            print("3.근처 SELECT END  =>  ", strDBGeoMasterSTOPS_ID)

            intSumOn_TNOPE = 0
            intSumOff_TNOPE = 0

            print("MASTER_ID ", strDBGeoMasterSTOPS_ID, strBusMasterSBWY_STNS_NM, strBusMasterSTOPS_TYPE)

            for rstSeoulBusSlaveGeo in rstSeoulBusSlaveGeos:
                print("3. --------------------------------------------------------")
                logging.info(SLog.Ins(Isp.getframeinfo,
                                      Isp.currentframe()) + "[  4. 버스 정류장 근처 LOOP START : ==========================")
                print("3. 버스 정류장 근처 LOOP START ---->")
                strBusSlaveSTOPS_ID = str(rstSeoulBusSlaveGeo.get('STOPS_ID'))
                strBusSlaveSBWY_STNS_NM = str(rstSeoulBusSlaveGeo.get('SBWY_STNS_NM'))
                strBusSlaveSTOPS_TYPE = str(rstSeoulBusSlaveGeo.get('STOPS_TYPE'))
                strBusSlaveON_TNOPE = str(rstSeoulBusSlaveGeo.get('GTON_TNOPE'))
                strBusSlaveOFF_TNOPE = str(rstSeoulBusSlaveGeo.get('GTOFF_TNOPE'))



                strListSLAVE_STOPS_ID += strBusSlaveSTOPS_ID + ","

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[  4. strBusSlaveSTOPS_ID : " + str(
                        strBusSlaveSTOPS_ID))
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[  4. strBusSlaveSBWY_STNS_NM : " + str(
                        strBusSlaveSBWY_STNS_NM))

                print("4. SLAVE 의 현재 상태 확인 START  =>  ", strDBGeoMasterSTOPS_ID , strBusSlaveSTOPS_ID)
                sqlSelectSlaveGeoDataForUpdate = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable
                sqlSelectSlaveGeoDataForUpdate += " WHERE STOPS_ID = %s"
                cursorRealEstate.execute(sqlSelectSlaveGeoDataForUpdate, (strBusSlaveSTOPS_ID))
                rstSelectSlaveGeoDataForUpdate = cursorRealEstate.fetchone()
                strSelectSlaveGeoDataForUpdate_master_flag = rstSelectSlaveGeoDataForUpdate.get('master_flag')
                print("4. SLAVE 의  현재 상태 확인 END =>  ", strDBGeoMasterSTOPS_ID, strBusSlaveSTOPS_ID , strSelectSlaveGeoDataForUpdate_master_flag )
                data_2 = strListSLAVE_STOPS_ID

                if strSelectSlaveGeoDataForUpdate_master_flag == '10':
                    print("5.MASTER 라서 업데이트 안함 =>")
                    print("NO UPDATE 10", strBusSlaveSTOPS_ID, strBusSlaveSBWY_STNS_NM, strBusSlaveSTOPS_TYPE,
                          strSelectSlaveGeoDataForUpdate_master_flag)
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[   5. 업데이트 안함 (10)---> ")
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[   5. strBusSlaveSTOPS_ID (10) : " + str(
                            strBusSlaveSTOPS_ID))
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[   5. strBusSlaveSBWY_STNS_NM (10) : " + str(
                            strBusSlaveSBWY_STNS_NM))
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[   5. strBusSlaveSTOPS_TYPE (10) : " + str(
                            strBusSlaveSTOPS_TYPE))
                    continue

                elif strSelectSlaveGeoDataForUpdate_master_flag == '01':
                    print("5.SLAVE 라서 업데이트 안함 =>")
                    print("NO UPDATE 01", strBusSlaveSTOPS_ID, strBusSlaveSBWY_STNS_NM, strBusSlaveSTOPS_TYPE,
                          strSelectSlaveGeoDataForUpdate_master_flag)
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[    5. 업데이트 안함 (01)---> ")
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[    5. strBusSlaveSTOPS_ID (01) : " + str(
                            strBusSlaveSTOPS_ID))
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[    5. strBusSlaveSBWY_STNS_NM (01) : " + str(
                            strBusSlaveSBWY_STNS_NM))
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[    5. strBusSlaveSTOPS_TYPE : (01) " + str(
                            strBusSlaveSTOPS_TYPE))
                    continue

                else:

                    print("5.업데이트 함!! =================>")
                    #--> 여기부터

                    strSelectMASTER_STOPS_ID = getMasterID(cursorRealEstate, strDBGeoMasterSTOPS_ID)

                    print("5.strSelectMASTER_STOPS_ID : ", strSelectMASTER_STOPS_ID)
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[    5. strSelectMASTER_STOPS_ID : " + str(
                            strSelectMASTER_STOPS_ID))


                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[    5. strSelectMASTER_STOPS_ID : " + str(
                            strSelectMASTER_STOPS_ID) + " | TYPE =" + str(type(strSelectMASTER_STOPS_ID)))
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[    5. strBusSlaveSTOPS_ID : " + str(
                            strBusSlaveSTOPS_ID) +" | TYPE =" +str(type(strBusSlaveSTOPS_ID)))

                    if strSelectMASTER_STOPS_ID == strBusSlaveSTOPS_ID:
                        strMasterFlag = '10'
                        intMasterCount += 1
                        data_5 = str(intMasterCount)
                    else:
                        strMasterFlag = '01'
                        intSlaveCount += 1
                        data_6 = str(intSlaveCount)


                    print("7. SLAVE 업데이트 함!! START =================>")
                    print("UPDATE", strBusSlaveSTOPS_ID, strBusSlaveSBWY_STNS_NM, strBusSlaveSTOPS_TYPE , strMasterFlag , intSumOn_TNOPE , intSumOff_TNOPE)
                    sqlUpdateBusSlave = " UPDATE "+ ConstRealEstateTable_GOV.SeoulBusGeoDataTable+ " SET "
                    sqlUpdateBusSlave += " MASTER_STOPS_ID = %s "
                    sqlUpdateBusSlave += " ,SLAVE_STOPS_ID = %s "
                    sqlUpdateBusSlave += " ,master_flag = %s "
                    sqlUpdateBusSlave += " ,modify_date = NOW() "
                    sqlUpdateBusSlave += " WHERE STOPS_ID = %s"
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[      7. strSelectMASTER_STOPS_ID : " + str(
                            strSelectMASTER_STOPS_ID))
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[      7. strMasterFlag : " + str(
                            strMasterFlag))
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[      7. strBusSlaveSTOPS_ID : " + str(
                            strBusSlaveSTOPS_ID))
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[     7. SLAVE 업데이트 함!! END: " + str(
                            strBusSlaveSTOPS_ID))
                    cursorRealEstate.execute(sqlUpdateBusSlave, (strSelectMASTER_STOPS_ID, strListSLAVE_STOPS_ID, strMasterFlag, strBusSlaveSTOPS_ID))
                    ResRealEstateConnection.commit()
                    print("7. SLAVE 업데이트 함!! END =================>")

                print("3. 버스 정류장 근처 LOOP END ---->")

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

        print("4. 버스 정류장 근처 LOOP END ---->")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except Exception as e:

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

    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[QuitException QuitException]")
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
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "[UpdateMasterCount END]=============================")
        ResRealEstateConnection.close()


def UpdateGeoRanking_1():


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
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "[main START]=============================")


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


        #DB 연결 선언
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        strDBUSE_YMD = GetLastData(cursorRealEstate, dtNow)

        print("strDBUSE_YMD => ", strDBUSE_YMD)
        strDBUSE_YM = str(strDBUSE_YMD)[:6]
        strDBUSE_Y = str(strDBUSE_YMD)[:4]


        sqlSelectStaticData  = " SELECT  STOPS_ID, MAX(USE_YMD) AS USE_YMDS,"
        sqlSelectStaticData += " SUM(GTON_TNOPE) AS  GTON_TNOPES, SUM(GTOFF_TNOPE) AS GTOFF_TNOPES "
        sqlSelectStaticData += " FROM kt_realty_seoul_bus_using_static_"+ strDBUSE_Y
        sqlSelectStaticData += " WHERE USE_YMD LIKE %s "
        sqlSelectStaticData += " GROUP BY STOPS_ID"
        sqlSelectStaticData += " ORDER BY GTON_TNOPES DESC"

        strLikeValue = (strDBUSE_YM + "%")
        cursorRealEstate.execute(sqlSelectStaticData, (strLikeValue))
        rstSeoulBusStatisticsDatas = cursorRealEstate.fetchall()

        intRank = 1
        intUpdateProcessCount = 0
        intSkipProcessCount = 0
        for rstSeoulBusStatisticsData in rstSeoulBusStatisticsDatas:
            print("rstSeoulBusStatisticsData => ", rstSeoulBusStatisticsData)

            if (intRank % 10) == 0:
                time.sleep(1)

            strRank = str(intRank)
            strDBGTON_TNOPES    = str(int(rstSeoulBusStatisticsData.get('GTON_TNOPES')))
            data_2 = strDBGTON_TNOPES
            strDBGTOFF_TNOPES   = str(int(rstSeoulBusStatisticsData.get('GTOFF_TNOPES')))
            data_3 = strDBGTOFF_TNOPES
            strDBSTOPS_ID       = str(rstSeoulBusStatisticsData.get('STOPS_ID'))
            data_1 = strDBSTOPS_ID
            sqlSelectBusStopGeodata  = " SELECT * FROM " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable + "  "
            sqlSelectBusStopGeodata += " WHERE STOPS_ID = %s "
            cursorRealEstate.execute(sqlSelectBusStopGeodata, (strDBSTOPS_ID))
            intSelectedCount = cursorRealEstate.rowcount

            if intSelectedCount == 1:

                listUpdateData = []
                listUpdateData.append(strRank)
                listUpdateData.append(strDBGTON_TNOPES)
                listUpdateData.append(strDBGTOFF_TNOPES)
                listUpdateData.append(strDBSTOPS_ID)

                print("listUpdateData => ", listUpdateData)

                sqlUpdateGeoRanking  = " UPDATE " + ConstRealEstateTable_GOV.SeoulBusGeoDataTable + " SET "
                sqlUpdateGeoRanking += " today_rank = %s"
                sqlUpdateGeoRanking += " ,GTON_TNOPE = %s"
                sqlUpdateGeoRanking += " ,GTOFF_TNOPE = %s"
                sqlUpdateGeoRanking += " WHERE STOPS_ID = %s "

                print("sqlUpdateGeoRanking => ", sqlUpdateGeoRanking)

                cursorRealEstate.execute(sqlUpdateGeoRanking, listUpdateData)

                ResRealEstateConnection.commit()
                intUpdateProcessCount += 1
                data_4 = intUpdateProcessCount


            else:
                intSkipProcessCount += 1
                data_5 = intSkipProcessCount
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[intSelectedCount]" + str(intSelectedCount))
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDBSTOPS_ID]" + str(strDBSTOPS_ID))

            data_6 = intRank
            intRank += 1



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
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "[MAIN END]=============================")
        ResRealEstateConnection.close()



def main():
    UpdateGeoRanking_1()
    # UpdateMasterID_2()
    UpdateMasterCount_3()

if __name__ == '__main__':
    main()
