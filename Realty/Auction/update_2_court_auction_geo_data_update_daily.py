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

    try:
        print(GetLogDef.lineno(__file__), "============================================================")

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

            strStripFieldName = str(strFieldName).removeprefix("[\"").removesuffix("\"]")
            print(GetLogDef.lineno(__file__), "strStripFieldName>", len(strStripFieldName), strStripFieldName)

            dictAddresses = str(strStripFieldName).split(":")
            print(GetLogDef.lineno(__file__), "dictAddresses", type(dictAddresses), len(dictAddresses), dictAddresses)

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

                if resultsDict != False:

                    # 네이버 데이터 조회 성공
                    print(GetLogDef.lineno(__file__), "resultsDict>", type(resultsDict), resultsDict)
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

            qryUpdateGeoPosition = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " SET "
            qryUpdateGeoPosition += " text_address = %s, "
            qryUpdateGeoPosition += " longitude = %s, "
            qryUpdateGeoPosition += " latitude = %s, "
            qryUpdateGeoPosition += " geo_point = ST_GeomFromText('POINT("+strLongitude+" "+strLatitude+")'), "
            qryUpdateGeoPosition += " process_step = %s "
            qryUpdateGeoPosition += " WHERE seq = %s "

            print("qryInfoUpdate", qryUpdateGeoPosition, type(qryUpdateGeoPosition))
            print("data", strJiBunAddress, strLongitude, strLatitude, nProcessStep, nSequence)
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
        err_msg = traceback.format_exc()
        print(err_msg)
        print(type(v))

    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.lineno(__file__), "QuitException")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        print(GetLogDef.lineno(__file__), "Error Exception")
        print(GetLogDef.lineno(__file__), e, type(e))
        err_msg = traceback.format_exc()
        print(err_msg)

    else:
        print(GetLogDef.lineno(__file__), "============================================================")

    finally:
        print("Finally END")
