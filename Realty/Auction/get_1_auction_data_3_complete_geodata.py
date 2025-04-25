# 동별로 수집시 IP 차단당함.
# 시군구단위로 수집 해야함. - 20231110
#

import requests

# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import logging
import logging.handlers
import inspect
import traceback
import re

import datetime

sys.path.append("D:/PythonProjects/airstock")

# from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Realty.Auction.Const import ConstRealEstateTable_AUC
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Realty.Auction.Const import AuctionCourtInfo
from Init.DefConstant import ConstRealEstateTable
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.CryptoModule import AesCrypto
from Realty.Auction.AuctionLib import MakeAuctionUniqueKey
import Realty.Auction.AuctionLib.AuctionDataDecode as AuctionDataDecode
import Realty.Government.MolitLib.GetRoadNameJuso as GetRoadNameJuso
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Lib.CustomException.QuitException import QuitException
from Realty.Auction.AuctionLib import AuctionMakeRequestHeader
from shapely.geometry import Point
from Lib.GeoDataModule import GeoDataModule


def main():
    try:

        # https://curlconverter.com/ <- 프로그램 컨버터

        # 물건상세 검색
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf"

        # 매각예정물건
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveMgakPlanMulSrch.laf"

        # 매각결과
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMgakGyulgwaMulList.laf"

        strProcessType = '023120'

        data_1 = '00'
        data_2 = '00'
        data_3 = '0'
        data_4 = '0'
        data_5 = '00'
        data_6 = '00'

        dtNow = DateTime.today()
        dtTimeBefore1Min = DateTime.today() - TimeDelta(seconds=5)
        strTimeStamp = str(dtTimeBefore1Min.timestamp()).replace(".", "")[0:13]
        # print(dtNow.hour)
        # print(dtNow.minute)
        print("strTimeStamp => " , strTimeStamp)

        LogPath = 'CourtAuction/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()
        strAddressSiguSequence='0'

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+
                     "[CRONTAB START]============================================================")


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        data_1 = strAddressSiguSequence = str(rstResult.get('data_1'))
        if strResult is False:
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'It is currently in operation. => ' + str(
                strResult))  # 예외를 발생시킴

        # if strResult == '20':
        #     data_1 = strAddressSiguSequence = str(rstResult.get('data_1'))

        if strResult == '40':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + '경매 서비스 점검 ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        # 초기 값
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        strMasterTable = ConstRealEstateTable_AUC.CourtAuctionCompleteTable

        qrySelectSeoulTradeMaster = f"SELECT * FROM {strMasterTable} "
        qrySelectSeoulTradeMaster += " WHERE state='00' "
        qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
        # qrySelectSeoulTradeMaster += " LIMIT 5 "
        cursorRealEstate.execute(qrySelectSeoulTradeMaster)
        rstSpoolDatas = cursorRealEstate.fetchall()
        data_5 = str(cursorRealEstate.rowcount)
        print("rstSiDoLists =>", qrySelectSeoulTradeMaster)

        intProcessLoop = 0
        for rstSpoolData in rstSpoolDatas:

            logging.info("")

            data_1 = strAuctionMasterSequence = str(rstSpoolData.get('seq'))

            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("MasterSeq >>")
            listLogData.append(str(strAuctionMasterSequence))
            listLogData.append("=============================================rstSpoolData")

            logging.info(f"%s [%s]%s %s", *listLogData)

            data_2 = hjguSido = str(rstSpoolData.get('hjguSido')).strip()
            data_3 = hjguSigu = str(rstSpoolData.get('hjguSigu')).strip()
            data_4 = hjguDong = str(rstSpoolData.get('hjguDong')).strip()
            hjguRd = str(rstSpoolData.get('hjguRd')).strip()
            daepyoLotno = str(rstSpoolData.get('daepyoLotno')).strip()


            ADDRESS_CODE = str(rstSpoolData.get('daepyoSidoCd')).strip()
            ADDRESS_CODE += str(rstSpoolData.get('daepyoSiguCd')).strip()
            ADDRESS_CODE += str(rstSpoolData.get('daepyoDongCd')).strip()
            ADDRESS_CODE += str(rstSpoolData.get('daepyoRdCd')).strip()

            dictAddress = dict()
            dictAddress[0] = hjguSido
            dictAddress[1] = hjguSigu
            dictAddress[2] = hjguDong
            dictAddress[3] = hjguRd
            dictAddress[4] = daepyoLotno

            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("dictAddress")
            listLogData.append(str(type(dictAddress)))
            listLogData.append(dictAddress)
            logging.info(f"%s [%s]%s %s", *listLogData)

            strAddressText = ''
            for strKey, strValue in dictAddress.items():
                if len(strValue) > 0:
                    strAddressText += strValue
                    strAddressText += ' '

            strAddressText = strAddressText.strip()

            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("strAddressText >> ")
            listLogData.append(strAddressText)
            logging.info(f"%s [%s][%s]", *listLogData)


            #juso.org 에서 동일한 주소 받아 오기
            dictAddressReturn = GetRoadNameJuso.GetConversionJuso(logging, strAddressText, ADDRESS_CODE)

            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("dictAddressReturn >> ")
            listLogData.append(str(type(dictAddressReturn)))
            listLogData.append(str(len(dictAddressReturn)))
            listLogData.append(dictAddressReturn)
            logging.info(f"%s [%s][%s](%s) %s", *listLogData)

            if dictAddressReturn['result'] == '30':
                listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                listLogData.append("continue >> ")
                listLogData.append(str(type(dictAddressReturn)))
                listLogData.append(dictAddressReturn)
                logging.info(f"%s [%s]%s %s", *listLogData)
                continue

            if dictAddressReturn['result'] == '10':
                ADDRESS_CODE = str(rstSpoolData.get('ADDRESS_CODE'))

            bInsertFlag = True
            strCourtAuctionAddressTable = ConstRealEstateTable_AUC.CourtAuctionAddressTable  # 테이블 이름을 변수에 저장 (가정)
            #경매 주소 테이블 중복 조회 예방
            sqlSelectMasterTable = f"SELECT * FROM {strCourtAuctionAddressTable} "
            sqlSelectMasterTable += f" WHERE jibunAddr = %s "


            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("sqlSelectMasterTable")
            listLogData.append(sqlSelectMasterTable)
            listLogData.append(dictAddressReturn['jibunAddr'])
            logging.info(f"%s [%s][%s](%s)", *listLogData)


            cursorRealEstate.execute(sqlSelectMasterTable, (dictAddressReturn['jibunAddr']))
            intSelectedCount = cursorRealEstate.rowcount


            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("SELECT jibunAddr >> ")
            listLogData.append(str(type(sqlSelectMasterTable)))
            listLogData.append(sqlSelectMasterTable)
            logging.info(f"%s [%s][%s]=>[%s]", *listLogData)

            if intSelectedCount > 0:
                listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                listLogData.append("SELECT jibunAddr >> ")
                listLogData.append(str(type(dictAddressReturn)))
                listLogData.append(dictAddressReturn)
                logging.info(f"%s [%s]%s %s", *listLogData)

                rstSpoolData = cursorRealEstate.fetchone()
                strAddressText = dictAddressReturn['jibunAddr']
                strAddressSeq = str(rstSpoolData.get('seq'))
                strLatitude = rstSpoolData.get('lat')
                strLongtitude = rstSpoolData.get('lng')
                bInsertFlag = False
            else:

                sqlSelectMasterTable = f"SELECT * FROM {strCourtAuctionAddressTable} "
                sqlSelectMasterTable += " WHERE roadAddr = %s "
                cursorRealEstate.execute(sqlSelectMasterTable, (dictAddressReturn['roadAddr']))
                intSelectedCount = cursorRealEstate.rowcount
                if intSelectedCount > 0:

                    listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                    listLogData.append("SELECT roadAddr >> ")
                    listLogData.append(str(type(dictAddressReturn)))
                    listLogData.append(dictAddressReturn)
                    logging.info(f"%s [%s]%s %s", *listLogData)

                    rstSpoolData = cursorRealEstate.fetchone()
                    strAddressText = dictAddressReturn['roadAddr']
                    strAddressSeq = str(rstSpoolData.get('seq'))
                    strLatitude = rstSpoolData.get('lat')
                    strLongtitude = rstSpoolData.get('lng')
                    bInsertFlag = False


            if bInsertFlag == True:

                listLogData = ["NAVER"]
                logging.info(f"[%s]", *listLogData)


                # 네이버 데이터 조회 시도
                resultsDict = GeoDataModule.getNaverGeoData(strAddressText)
                if isinstance(resultsDict, dict) != False:

                    for resultsOneDictKey, resultsOneDictValue in resultsDict.items():

                        listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                        listLogData.append(resultsOneDictKey)
                        listLogData.append(resultsOneDictValue)
                        logging.info(f"%s[%s=>%s]", *listLogData)

                    listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                    listLogData.append("resultsDict")
                    listLogData.append(resultsDict)
                    logging.info(f"%s[%s=>%s]", *listLogData)

                    strJiBunAddress = resultsDict['address_name']
                    strRoadName = resultsDict['road_name']

                    strLongtitude = resultsDict['x']  # 127
                    strLatitude = resultsDict['y']  # 37
                    nProcessStep = 10

                else:
                    # 네이버 데이터 조회 실패
                    listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                    listLogData.append("SELECT roadAddr >> ")
                    listLogData.append(str(type(dictAddressReturn)))
                    listLogData.append(dictAddressReturn)
                    logging.info(f"%s [%s]%s %s", *listLogData)

                    # 카카오 데이터 조회 시도
                    resultsDict = GeoDataModule.getKakaoGeoData(strAddressText)
                    if isinstance(resultsDict, dict) != False:
                        # 카카오 데이터 조회 성공
                        strJiBunAddress = resultsDict['address_name']
                        strLongtitude = resultsDict['x']
                        strLatitude = resultsDict['y']
                        nProcessStep = 11


                # 주소 테이블 INSERT

                dictDataRow = dict()
                dictDataRow['hjguSido'] = hjguSido
                dictDataRow['hjguSigu'] = hjguSigu
                dictDataRow['hjguDong'] = hjguDong
                dictDataRow['hjguRd'] = hjguRd
                dictDataRow['daepyoLotno'] = daepyoLotno
                dictDataRow['ADDRESS_CODE'] = ADDRESS_CODE
                dictDataRow['jibunAddr'] = dictAddressReturn['jibunAddr']
                dictDataRow['roadAddr'] = dictAddressReturn['roadAddr']
                dictDataRow['lng'] = strLongtitude
                dictDataRow['lat'] = strLatitude

                point = Point(strLongtitude, strLatitude)
                wkt_point = str(point)

                # 컬럼 이름과 값 추출
                columns = ", ".join(dictDataRow.keys())
                columns += ", geo_point "

                values = ", ".join(["%s"] * len(dictDataRow))
                values += ', ST_GeomFromText(%s, 4326, "axis-order=long-lat" )'

                values_list = list(dictDataRow.values())
                values_list.append(wkt_point)

                sqlInsertAuctionAddress = f"INSERT INTO {strCourtAuctionAddressTable} ({columns}) VALUES ({values})"

                print("sqlInsertAuctionAddress => ", sqlInsertAuctionAddress)
                listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
                listLogData.append("sqlInsertAuctionAddress>> ")
                listLogData.append(sqlInsertAuctionAddress)
                listLogData.append(str(values_list))
                logging.info(f"%s [%s] (%s) %s", *listLogData)

                # 쿼리 실행
                cursorRealEstate.execute(sqlInsertAuctionAddress, values_list)
                strAddressSeq = str(cursorRealEstate.lastrowid)

            dictDataRow = dict()
            dictDataRow['lng'] = strLongtitude
            dictDataRow['lat'] = strLatitude
            dictDataRow['address_seq'] = strAddressSeq
            dictDataRow['state'] = '10'

            point = Point(strLongtitude, strLatitude)
            wkt_point = str(point)

            # 컬럼 이름과 값 추출
            columns = ", ".join(dictDataRow.keys())
            columns += ", geo_point "

            values = ", ".join(["%s"] * len(dictDataRow))
            values += ', ST_GeomFromText(%s, 4326, "axis-order=long-lat" )'

            values_list = list(dictDataRow.values())
            values_list.append(wkt_point)

            strMasterTable = ConstRealEstateTable_AUC.CourtAuctionCompleteTable
            sqlUpdateAuctionMaster = f"UPDATE {strMasterTable} SET "
            sqlUpdateAuctionMaster += " geo_point = ST_GeomFromText('POINT(" + strLongtitude + " " + strLatitude + ")', 4326,'axis-order=long-lat')"
            sqlUpdateAuctionMaster += " , modify_date = NOW() "


            for strDataRowKey, strDataRowValue in dictDataRow.items():
                sqlUpdateAuctionMaster += f" , {strDataRowKey} ='{strDataRowValue}' "
            sqlUpdateAuctionMaster += " WHERE seq= %s "
            cursorRealEstate.execute(sqlUpdateAuctionMaster, (strAuctionMasterSequence))

            listLogData = [SLog.Ins(Isp.getframeinfo, Isp.currentframe())]
            listLogData.append("sqlUpdateAuctionMaster>> ")
            listLogData.append(sqlUpdateAuctionMaster)
            listLogData.append(str(strAuctionMasterSequence))
            logging.info(f"%s [%s] (%s) %s", *listLogData)

            #변경 사항 커밋
            ResRealEstateConnection.commit()

            dtTimeDifference = DateTime.now() - TimeDelta(hours=dtNow.hour, minutes=dtNow.minute, seconds=dtNow.second)
            dtWorkingTime = str(dtTimeDifference.strftime('%H:%M:%S'))

            intProcessLoop += 1
            data_6 = str(intProcessLoop)
            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = data_1
            dictSwitchData['data_2'] = data_2
            dictSwitchData['data_3'] = data_3
            dictSwitchData['data_4'] = data_4
            dictSwitchData['data_5'] = data_5
            dictSwitchData['data_6'] = data_6
            dictSwitchData['working_time'] = dtWorkingTime
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            time.sleep(0.5)
        print("for rstSiDoList in rstSiDoLists: END")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    except Exception as e:

        ResRealEstateConnection.rollback()

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
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

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                     "[=============[SUCCESS END]")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                     "[=============[CRONTAB END]")
        cursorRealEstate.close()  # cursor를 닫아줘야합니다.
        return True

if __name__ == '__main__':
    main()
