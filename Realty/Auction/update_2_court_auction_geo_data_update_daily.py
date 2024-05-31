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
import inspect

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
import Realty.Auction.AuctionLib.AuctionDataDecode as AuctionDataDecode
import Realty.Government.MolitLib.GetRoadNameJuso as GetRoadNameJuso

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

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno),
              "START => ============================================================")

        logging.info("[CRONTAB START : " + inspect.getframeinfo(inspect.currentframe()).filename + "]=====================================")


        # 서울 부동산 실거래가 데이터 - 서울 버스 사용량
        strProcessType = '020101'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'


        # 스위치 데이터 조회 type(000200) result (10:정기점검)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2('000200')
        strResult = rstResult.get('result')
        if strResult is False:
            quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴

        if strResult == '10':
            process_start_date = rstResult.get('process_start_date').strftime('%Y-%m-%d %H:%M:%S')
            last_date = rstResult.get('last_date').strftime('%Y-%m-%d %H:%M:%S')
            dtRegNow = DateTime.today()
            process_start_date_obj = DateTime.strptime(process_start_date, '%Y-%m-%d %H:%M:%S')
            last_date_obj = DateTime.strptime(last_date, '%Y-%m-%d %H:%M:%S')

            if (process_start_date_obj <= dtRegNow) and (dtRegNow <= last_date_obj):
                print("process_start_date >> ", process_start_date)
                print("dtRegNow >> ", dtRegNow)
                print("last_date >> ", last_date)
                quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴



        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno))  # 예외를 발생시킴

        if strResult == '10':
            raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno))  # 예외를 발생시킴

        # if strResult == '30':
        #     raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                             inspect.getframeinfo(inspect.currentframe()).lineno))  # 예외를 발생시킴

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


        #CourtAuctionDataTable
        qrySelectCourtAuctionMaster = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " WHERE "
        qrySelectCourtAuctionMaster += " process_step = '00' "  # 데이터 있음

        # qrySelectCourtAuctionMaster += " process_step = '13' AND auction_type!='30' "  # 데이터 있음
        # qrySelectCourtAuctionMaster += " process_step = '13' AND auction_type!='30' "  # 데이터 있음
        # qrySelectCourtAuctionMaster += " seq='2722742' " #데이터 있음
        # qrySelectCourtAuctionMaster += " limit 2"


        cursorRealEstate.execute(qrySelectCourtAuctionMaster)
        rstFieldsLists = cursorRealEstate.fetchall()

        print(GetLogDef.lineno(__file__), "rstFieldsList>", rstFieldsLists)

        nTotalCount = 0
        nProcessCount = 0
        strJiBunAddress = ''
        strLongitude = '000.00000000'  # 127
        strLatitude = '000.00000000'  # 37
        nProcessStep = 13

        for SelectColumnList in rstFieldsLists:



            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno),
                  "------------------------------------------------------------------------------------------------------------")

            nSequence = SelectColumnList.get('seq')

            strFieldName = SelectColumnList.get('address_data')
            strDBIssueNumberText = SelectColumnList.get('issue_number_text')
            strDBTextAddress =  SelectColumnList.get('address_data_text')

            strIssueNumber = AuctionDataDecode.DecodeIssueNumber(strDBIssueNumberText)
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), len(strIssueNumber),
                  strIssueNumber)



            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "nSequence> " +  str(nSequence) + " / strFieldName> " +  str(strFieldName) + "/ len(strFieldName) > " +  str(len(strFieldName)))


            strAuctionCode = SelectColumnList.get('auction_code')
            strCourtName = SelectColumnList.get('court_name')
            strAuctionDay = SelectColumnList.get('auction_day')

            # print(GetLogDef.lineno(__file__), "strFieldName >", type(strFieldName), strFieldName)
            jsonFieldName = json.loads(strFieldName)
            # print(GetLogDef.lineno(__file__), "jsonFieldName >", type(jsonFieldName), jsonFieldName)
            # print(GetLogDef.lineno(__file__), "jsonFieldName[0] >", type(jsonFieldName[0]), jsonFieldName[0])

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "jsonFieldName> "+ str(jsonFieldName) + str(len(jsonFieldName)))

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno),
                  "nSequence>", nSequence, strFieldName,len(strFieldName))

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "nSequence>"+ str(nSequence) + str(strFieldName) + str(len(strFieldName)))

            strStripFieldName = str(strFieldName).removeprefix("[\"").removesuffix("\"]")
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strStripFieldName>", len(strStripFieldName), strStripFieldName)

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "strStripFieldName>"+str(len(strStripFieldName)) + str(strStripFieldName))


            dictAddresses = str(strStripFieldName).split(":")
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "dictAddresses", type(dictAddresses), len(dictAddresses), dictAddresses)

            for dictAddresse in dictAddresses:
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + " dictAddresse >> " + dictAddresse)

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + " strStripFieldName >> " + str(len(strStripFieldName)))


            if len(strStripFieldName) > 5:

                #주소 예외 처리["사용본거지:경기도수원시권선구평동110-5"]
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + " dictAddresses[0] >> " + str(dictAddresses[0]))
                if dictAddresses[0].strip() == '사용본거지':
                    jsonFieldName[0] = dictAddresses[1]

                strTextAddress = str(jsonFieldName[0]).split(",")

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "strTextAddres>"+ str(len(strTextAddress))+ str(strTextAddress))


            else:

                if len(strStripFieldName) < 1:
                    strStripFieldName = "False"

                strTextAddress = dict()
                strTextAddress[0] = strStripFieldName

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "strTextAddres> " + str(len(strTextAddress)) + str(strTextAddress))


            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strTextAddress>", type(strTextAddress), len(strTextAddress), strTextAddress)

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strFieldName>", type(strFieldName), len(strFieldName), strFieldName)


            for strTextAddres in strTextAddress:

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "strTextAddres>"+ str(strTextAddres))


            if len(strFieldName) < 10:
                #주소가 없을떄 업데이트 SKIP
                strJiBunAddress = ''
                strLongitude = '000.00000000'  # 127
                strLatitude = '000.00000000'  # 37
                nProcessStep = 13

            else:

                print("strTextAddress[0]================>" ,strTextAddress[0])

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                        inspect.getframeinfo(inspect.currentframe()).lineno), len(strTextAddress),
                      strTextAddress)

                strTextAddressReturn = GetRoadNameJuso.GetJusoApiForAddress(logging, strIssueNumber, strDBTextAddress)
                print("strTextAddressReturn================>" ,strTextAddressReturn)


                # 네이버 데이터 조회 시도
                resultsDict = GeoDataModule.getNaverGeoData(strTextAddress[0])
                if isinstance(resultsDict, dict) != False:

                    for resultsOneDictKey, resultsOneDictValue in resultsDict.items():
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) +  " resultsDict =>", type(resultsOneDictValue), resultsOneDictValue)
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + " resultsOneDict =>" + resultsOneDictKey + " > " + resultsOneDictValue)

                    # 네이버 데이터 조회 성공
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) , "resultsDict>", type(resultsDict), resultsDict)
                    strJiBunAddress = resultsDict['address_name']
                    strRoadName = resultsDict['road_name']

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) + " strJiBunAddress =>" + strJiBunAddress )


                    strLongitude = resultsDict['x']  # 127
                    strLatitude = resultsDict['y']  # 37
                    nProcessStep = 10

                else:
                    # 네이버 데이터 조회 실패

                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strTextAddress>", type(strTextAddress), strTextAddress)
                    # 카카오 데이터 조회 시도
                    resultsDict = GeoDataModule.getKakaoGeoData(strTextAddress[0])
                    if isinstance(resultsDict, dict) != False:
                        # 카카오 데이터 조회 성공
                        strJiBunAddress = resultsDict['address_name']
                        strLongitude = resultsDict['x']
                        strLatitude = resultsDict['y']
                        nProcessStep = 11


            strJiBunAddress = GetLogDef.stripSpecharsForText(strJiBunAddress)

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strJiBunAddress>", type(strJiBunAddress), strJiBunAddress)

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strRoadName>", type(strRoadName), strRoadName)

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strLongitude>", type(strLongitude), strLongitude)
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "strLatitude>", type(strLatitude), strLatitude)
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "nProcessStep>", type(nProcessStep), nProcessStep)
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "nSequence>",type(nSequence), nSequence)
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno)+" strJiBunAddress>"+  str(strJiBunAddress))
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno)+"strLongitude>" +  str(strLongitude))
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno)+ " strLatitude>" +  str(strLatitude))
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno)+ "nProcessStep>" + str(nProcessStep))
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "nSequence>" + str(nSequence))


            # qryUpdateGeoPosition = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionBackupTable + " SET "
            qryUpdateGeoPosition = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " SET "
            qryUpdateGeoPosition += " text_address = %s, "
            qryUpdateGeoPosition += " road_name = %s, "
            qryUpdateGeoPosition += " longitude = %s, "
            qryUpdateGeoPosition += " latitude = %s, "
            qryUpdateGeoPosition += " geo_point = ST_GeomFromText('POINT("+strLongitude+" "+strLatitude+")'), "
            qryUpdateGeoPosition += " process_step = %s "
            qryUpdateGeoPosition += " WHERE seq = %s "

            print("qryUpdateGeoPosition", qryUpdateGeoPosition, type(qryUpdateGeoPosition))
            print("data", strJiBunAddress,strRoadName, strLongitude, strLatitude, nProcessStep, nSequence)

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "qryUpdateGeoPosition>" + str(qryUpdateGeoPosition))


            cursorRealEstate.execute(qryUpdateGeoPosition, (strJiBunAddress, strRoadName, strLongitude, strLatitude, nProcessStep, nSequence))
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
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "ValueError>" + str(v))
        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "err_msg>" +  str(err_msg))

        print(type(v))

    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "QuitException")
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "QuitException>")

        err_msg = traceback.format_exc()
        print(err_msg)
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "err_msg>"  + str(err_msg))
        print(e)
        print(type(e))

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "Error Exception")

        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        err_msg = traceback.format_exc()
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), e, type(e))
        print(err_msg)
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "Exception>")
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + " err_msg>" + str(e))
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + " err_msg>" + str(err_msg))



    else:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno), "============================================================")

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + " SUCCESS => ============================================================")

    finally:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "Finally END")
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "Finally END => ============================================================")
