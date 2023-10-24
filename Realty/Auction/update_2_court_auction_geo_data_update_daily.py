# quit("500")
#

#https://console.ncloud.com/naver-service/application
#VPC AI·NAVER API Application

import requests


# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import datetime
sys.path.append("D:/PythonProjects/airstock")
import urllib.request
import traceback
import logging
import logging.handlers


from typing import Dict, Union, Optional

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV

from Realty.Auction.Const import ConstRealEstateTable_AUC
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Realty.Auction.Const import AuctionCourtInfo
from Init.DefConstant import ConstRealEstateTable
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.CustomException.QuitException import QuitException
from Lib.GeoDataModule import GeoDataModule


def main():
    dtNow = DateTime.today()
    # print(dtNow.hour)
    # print(dtNow.minute)
    # print(dtNow)

    logFileName = str(dtNow.year) + str(dtNow.month) + str(dtNow.day) + ".log"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')

    streamingHandler = logging.StreamHandler()
    streamingHandler.setFormatter(formatter)

    # RotatingFileHandler
    log_max_size = 10 * 1024 * 1024  ## 10MB
    log_file_count = 20
    rotatingFileHandler = logging.handlers.RotatingFileHandler(
        filename='D:/PythonProjects/airstock/Shell/logs/020101_' + logFileName,
        maxBytes=log_max_size,
        backupCount=log_file_count
    )
    rotatingFileHandler.setFormatter(formatter)
    # RotatingFileHandler
    timeFileHandler = logging.handlers.TimedRotatingFileHandler(
        filename='D:/PythonProjects/airstock/Shell/logs/020101_' + logFileName,
        when='midnight',
        interval=1,
        encoding='utf-8'
    )
    timeFileHandler.setFormatter(formatter)
    logger.addHandler(streamingHandler)
    logger.addHandler(timeFileHandler)

    try:
        print(GetLogDef.lineno(__file__), "============================================================")
        logging.info("[CRONTAB START : " + GetLogDef.lineno(__file__) + "]=====================================")


        # 서울 부동산 실거래가 데이터 - 서울 버스 사용량
        strProcessType = '020101'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise QuitException(GetLogDef.lineno(__file__))  # 예외를 발생시킴

        if strResult == '10':
            raise QuitException(GetLogDef.lineno(__file__))  # 예외를 발생시킴

        if strResult == '30':
            raise QuitException(GetLogDef.lineno(__file__))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        # qrySelectCourtAuctionMaster = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionBackupTable + " WHERE "
        # qrySelectCourtAuctionMaster += " seq='101804' " #데이터 없음
        # qrySelectCourtAuctionMaster += " seq='104781' " #데이터 있음


        qrySelectCourtAuctionMaster = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " WHERE process_step= '00' "
        # qrySelectCourtAuctionMaster += " limit 2"

        cursorRealEstate.execute(qrySelectCourtAuctionMaster)
        rstFieldsLists = cursorRealEstate.fetchall()

        # print(GetLogDef.lineno(__file__), "rstFieldsList>", rstFieldsLists)

        nTotalCount = 0
        nProcessCount = 0
        strJiBunAddress = ''
        strLongitude = '000.00000000'  # 127
        strLatitude = '000.00000000'  # 37
        nProcessStep = 13

        for SelectColumnList in rstFieldsLists:

            nSequence = SelectColumnList.get('seq')

            strFieldName = SelectColumnList.get('address_data')

            strAuctionCode = SelectColumnList.get('auction_code')
            strCourtName = SelectColumnList.get('court_name')
            strAuctionDay = SelectColumnList.get('auction_day')

            # print(GetLogDef.lineno(__file__), "strFieldName >", type(strFieldName), strFieldName)
            jsonFieldName = json.loads(strFieldName)
            # print(GetLogDef.lineno(__file__), "jsonFieldName >", type(jsonFieldName), jsonFieldName)
            # print(GetLogDef.lineno(__file__), "jsonFieldName[0] >", type(jsonFieldName[0]), jsonFieldName[0])
            print(GetLogDef.lineno(__file__), "------------------------------------------------------------------------------------------------------------")
            print(GetLogDef.lineno(__file__), "nSequence>", nSequence, strFieldName,len(strFieldName))
            logging.info(GetLogDef.lineno(__file__)+ "------------------------------------------------------------------------------------------------------------")
            logging.info(GetLogDef.lineno(__file__) + "nSequence>"+ str(nSequence) + str(strFieldName) + str(len(strFieldName)))

            strStripFieldName = str(strFieldName).removeprefix("[\"").removesuffix("\"]")
            print(GetLogDef.lineno(__file__), "strStripFieldName>", len(strStripFieldName), strStripFieldName)
            logging.info(GetLogDef.lineno(__file__)+ "strStripFieldName>"+str(len(strStripFieldName)) + str(strStripFieldName))


            dictAddresses = str(strStripFieldName).split(":")
            print(GetLogDef.lineno(__file__), "dictAddresses", type(dictAddresses), len(dictAddresses), dictAddresses)
            logging.info(GetLogDef.lineno(__file__)+ "dictAddresses"+ type(dictAddresses)+ str(len(dictAddresses)) + dictAddresses)


            if len(strStripFieldName) > 5:
                #주소 예외 처리["사용본거지:경기도수원시권선구평동110-5"]
                if dictAddresses[0] == '사용본거지':
                    jsonFieldName[0] = dictAddresses[1]

                strTextAddress = str(jsonFieldName[0]).split(",")
            else:
                strTextAddress = dict()
                strTextAddress[0] = strStripFieldName


            print(__file__, "strTextAddress>", type(strTextAddress), len(strTextAddress), strTextAddress)
            print(GetLogDef.lineno(__file__), "strFieldName>", type(strFieldName), len(strFieldName), strFieldName)
            logging.info(__file__, "strTextAddress>"+ type(strTextAddress)+ str(len(strTextAddress))+ strTextAddress)
            logging.info(GetLogDef.lineno(__file__) + "strFieldName>"+ type(strFieldName)+ str(len(strFieldName))+ strFieldName)

            if len(strFieldName) < 10:
                #주소가 없을떄 업데이트 SKIP
                strJiBunAddress = ''
                strLongitude = '000.00000000'  # 127
                strLatitude = '000.00000000'  # 37
                nProcessStep = 13

            else:

                # 네이버 데이터 조회 시도
                resultsDict = GeoDataModule.getNaverGeoData(strTextAddress[0])

                print("146", "resultsDict>", type(resultsDict), resultsDict)

                logging.info("146"+ "resultsDict>"+ type(resultsDict)+ resultsDict)


                if resultsDict != False:

                    # 네이버 데이터 조회 성공
                    print(GetLogDef.lineno(__file__), "resultsDict>", type(resultsDict), resultsDict)
                    logging.info("201" + "resultsDict>" + type(resultsDict) + resultsDict)

                    strJiBunAddress = resultsDict['address_name']
                    strLongitude = resultsDict['x']  # 127
                    strLatitude = resultsDict['y']  # 37
                    nProcessStep = 10

                else:
                    # 네이버 데이터 조회 실패
                    # print(GetLogDef.lineno(__file__), "resultsDict>", type(resultsDict), resultsDict)
                    # 카카오 데이터 조회 시도
                    resultsDict = GeoDataModule.getKakaoGeoData(strTextAddress)
                    if resultsDict != False:
                        # 카카오 데이터 조회 성공
                        strJiBunAddress = resultsDict['address_name']
                        strLongitude = resultsDict['x']
                        strLatitude = resultsDict['y']
                        nProcessStep = 11


            strJiBunAddress = GetLogDef.stripSpecharsForText(strJiBunAddress)

            print(GetLogDef.lineno(__file__), "strJiBunAddress>", type(strJiBunAddress), strJiBunAddress)
            print(GetLogDef.lineno(__file__), "strLongitude>", type(strLongitude), strLongitude)
            print(GetLogDef.lineno(__file__), "strLatitude>", type(strLatitude), strLatitude)
            print(GetLogDef.lineno(__file__), "nProcessStep>", type(nProcessStep), nProcessStep)
            print(GetLogDef.lineno(__file__), "nSequence>",type(nSequence), nSequence)

            logging.info("222 strJiBunAddress>"+ type(strJiBunAddress)+ str(strJiBunAddress))
            logging.info("223 strLongitude>" + type(strLongitude) + str(strLongitude))
            logging.info("224 strLatitude>" + type(strLatitude) + str(strLatitude))
            logging.info("225 nProcessStep>" + type(nProcessStep) + str(nProcessStep))
            logging.info("226 nSequence>" + type(nSequence) + str(nSequence))



            qryUpdateGeoPosition = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " SET "
            qryUpdateGeoPosition += " text_address = %s, "
            qryUpdateGeoPosition += " longitude = %s, "
            qryUpdateGeoPosition += " latitude = %s, "
            qryUpdateGeoPosition += " geo_point = ST_GeomFromText('POINT("+strLongitude+" "+strLatitude+")'), "
            qryUpdateGeoPosition += " process_step = %s "
            qryUpdateGeoPosition += " WHERE seq = %s "

            print("qryUpdateGeoPosition", qryUpdateGeoPosition, type(qryUpdateGeoPosition))
            print("data", strJiBunAddress, strLongitude, strLatitude, nProcessStep, nSequence)

            logging.info("248 qryUpdateGeoPosition>" + type(qryUpdateGeoPosition) + str(qryUpdateGeoPosition))


            cursorRealEstate.execute(qryUpdateGeoPosition, (strJiBunAddress, strLongitude, strLatitude, nProcessStep, nSequence))
            ResRealEstateConnection.commit()

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = strJiBunAddress
            dictSwitchData['data_2'] = strLongitude
            dictSwitchData['data_3'] = strLatitude
            dictSwitchData['data_4'] = nProcessStep
            dictSwitchData['data_5'] = nProcessCount
            # dictSwitchData['data_6'] = nCallEndCount

            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = strJiBunAddress
        dictSwitchData['data_2'] = strLongitude
        dictSwitchData['data_3'] = strLatitude
        dictSwitchData['data_4'] = nProcessStep
        dictSwitchData['data_5'] = nProcessCount
        # dictSwitchData['data_6'] = nCallEndCount

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    except ValueError as v:
        print(v)
        logging.info("280 ValueError>" + type(v) + str(v))
        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info("284 err_msg>" + type(err_msg) + str(err_msg))

        print(type(v))

    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.lineno(__file__), "QuitException")
        logging.info("294 QuitException>")

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info("298 err_msg>" + type(err_msg) + str(err_msg))
        print(e)
        print(type(e))

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        print(GetLogDef.lineno(__file__), "Error Exception")
        print(GetLogDef.lineno(__file__), e, type(e))

        logging.info("312 Exception>")
        logging.info("313 err_msg>" + type(e) + str(e))
        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info("298 err_msg>" + type(err_msg) + str(err_msg))
    else:
        print(GetLogDef.lineno(__file__), "============================================================")

        logging.info("320 SUCCESS => ============================================================")

    finally:
        print("Finally END")
        logging.info("Finally END => ============================================================")
