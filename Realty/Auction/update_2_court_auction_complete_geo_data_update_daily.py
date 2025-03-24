# quit("500")
#

#https://console.ncloud.com/naver-service/application
#VPC AI·NAVER API Application

import requests


# This is a sample Python script.
import sys
sys.path.append("D:/PythonProjects/airstock")
import json
import time
import random
import pymysql
import datetime

import urllib.request
import traceback

import inspect

from typing import Dict, Union, Optional

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup



def main():
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

    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF

    dtNow = DateTime.today()
    # print(dtNow.hour)
    # print(dtNow.minute)
    # print(dtNow)


    try:


        # 법원 경매 부동산 master 테이블 위도경도 업데이트
        strProcessType = '020102'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'
        LogPath = 'CourtAuction/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[CRONTAB START : =====================================]")

        # 스위치 데이터 조회 type(000200) result (10:정기점검)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2('000200')
        strResult = rstResult.get('result')
        if strResult is False:
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            process_start_date = rstResult.get('process_start_date').strftime('%Y-%m-%d %H:%M:%S')
            last_date = rstResult.get('last_date').strftime('%Y-%m-%d %H:%M:%S')
            dtRegNow = DateTime.today()
            process_start_date_obj = DateTime.strptime(process_start_date, '%Y-%m-%d %H:%M:%S')
            last_date_obj = DateTime.strptime(last_date, '%Y-%m-%d %H:%M:%S')

            if (process_start_date_obj <= dtRegNow) and (dtRegNow <= last_date_obj):
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[process_start_date_obj] >>" + process_start_date_obj)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dtRegNow] >>" + dtRegNow)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[last_date_obj] >>" + last_date_obj)
                quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            raise QuitException.QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        if strResult == '10':
            raise QuitException.QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

        # if strResult == '30':
        #     raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                             inspect.getframeinfo(inspect.currentframe()).lineno))  # 예외를 발생시킴


        if strResult == '40':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

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
        qrySelectCourtAuctionMaster = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable + " WHERE "
        qrySelectCourtAuctionMaster += " process_step = '00' "  # 데이터 있음

        # qrySelectCourtAuctionMaster += " AND auction_type != '30' "  # 데이터 있음



        # qrySelectCourtAuctionMaster += " process_step = '13' AND auction_type!='30' "  # 데이터 있음
        # qrySelectCourtAuctionMaster += " process_step = '13' AND auction_type!='30' "  # 데이터 있음
        # qrySelectCourtAuctionMaster += " seq='2722742' " #데이터 있음
        # qrySelectCourtAuctionMaster += " limit 2"


        cursorRealEstate.execute(qrySelectCourtAuctionMaster)
        rstFieldsLists = cursorRealEstate.fetchall()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstFieldsLists] >>" + str(rstFieldsLists))

        nTotalCount = 0
        nProcessCount = 0
        strJiBunAddress = ''
        strLongitude = '000.00000000'  # 127
        strLatitude = '000.00000000'  # 37
        nProcessStep = 13

        for SelectColumnList in rstFieldsLists:

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + "[------------------------------------------------------------------------------------------------------------]")

            nSequence = SelectColumnList.get('seq')

            strFieldName = SelectColumnList.get('address_data')
            strDBIssueNumberText = SelectColumnList.get('issue_number_text')
            strDBTextAddress =  SelectColumnList.get('address_data_text')

            strIssueNumber = AuctionDataDecode.DecodeIssueNumber(strDBIssueNumberText)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strIssueNumber] >>" + str(strIssueNumber))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[nSequence] >>" + str(nSequence))
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strFieldName] >>" + str(strFieldName))


            strAuctionCode = SelectColumnList.get('auction_code')
            strCourtName = SelectColumnList.get('court_name')
            strAuctionDay = SelectColumnList.get('auction_day')

            # print(GetLogDef.lineno(__file__), "strFieldName >", type(strFieldName), strFieldName)
            jsonFieldName = json.loads(strFieldName)
            # print(GetLogDef.lineno(__file__), "jsonFieldName >", type(jsonFieldName), jsonFieldName)
            # print(GetLogDef.lineno(__file__), "jsonFieldName[0] >", type(jsonFieldName[0]), jsonFieldName[0])


            strStripFieldName = str(strFieldName).removeprefix("[\"").removesuffix("\"]")
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strStripFieldName] >>" + str(strStripFieldName))


            dictAddresses = str(strStripFieldName).split(":")
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dictAddresses] >>" + str(dictAddresses))

            for dictAddresse in dictAddresses:
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dictAddresse] >>" + str(dictAddresse))

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strStripFieldName] >>" + str(strStripFieldName))

            boolDongsan = False

            if len(strStripFieldName) > 5:

                #주소 예외 처리["사용본거지:경기도수원시권선구평동110-5"]
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dictAddresses[0]] >>" + str(dictAddresses[0]))

                if dictAddresses[0].strip() == '사용본거지':
                    jsonFieldName[0] = dictAddresses[1]
                    boolDongsan = True

                strTextAddress = str(jsonFieldName[0]).split(",")
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTextAddress] >>" + str(strTextAddress))


            else:

                if len(strStripFieldName) < 1:
                    strStripFieldName = "False"

                strTextAddress = dict()
                strTextAddress[0] = strStripFieldName
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTextAddress[0]] >>" + str(strTextAddress[0]))

            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTextAddress] >>" + str(strTextAddress))
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strFieldName] >>" + str(strFieldName))


            for strTextAddres in strTextAddress:
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTextAddres] >>" + str(strTextAddres))


            if len(strFieldName) < 10:
                #주소가 없을떄 업데이트 SKIP
                strJiBunAddress = ''
                strLongitude = '000.00000000'  # 127
                strLatitude = '000.00000000'  # 37
                nProcessStep = 13

            else:

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTextAddress[0]] >>" + str(strTextAddress[0]))

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTextAddress] >>" + str(strTextAddress))

                strTextAddressReturn = GetRoadNameJuso.GetJusoApiForAddress(logging, strIssueNumber, strDBTextAddress)
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTextAddressReturn] >>" + str(strTextAddressReturn))


                dictConversionAddress = GetRoadNameJuso.GetDictConversionAddress(logging, strIssueNumber,strDBTextAddress)
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dictConversionAddress] >>" + str(dictConversionAddress))

                admCd = dictConversionAddress['admCd']

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[admCd] >>" + str(admCd))

                strDongmyunCode = admCd[5:10]
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDongmyunCode] >>" + str(strDongmyunCode))

                # strDongmyunCode = '00000'

                # 네이버 데이터 조회 시도
                resultsDict = GeoDataModule.getNaverGeoData(strTextAddress[0])
                if isinstance(resultsDict, dict) != False:

                    for resultsOneDictKey, resultsOneDictValue in resultsDict.items():
                        logging.info(
                            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[resultsOneDictKey] >>" + str(resultsOneDictKey))


                    # 네이버 데이터 조회 성공
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[resultsDict] >>" + str(resultsDict))

                    strJiBunAddress = resultsDict['address_name']
                    strRoadName = resultsDict['road_name']
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strJiBunAddress] >>" + str(strJiBunAddress))


                    strLongitude = resultsDict['x']  # 127
                    strLatitude = resultsDict['y']  # 37
                    nProcessStep = 10

                else:
                    # 네이버 데이터 조회 실패
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strTextAddress] >>" + str(strTextAddress))

                    # 카카오 데이터 조회 시도
                    resultsDict = GeoDataModule.getKakaoGeoData(strTextAddress[0])
                    if isinstance(resultsDict, dict) != False:
                        # 카카오 데이터 조회 성공
                        strJiBunAddress = resultsDict['address_name']
                        strLongitude = resultsDict['x']
                        strLatitude = resultsDict['y']
                        nProcessStep = 11


            strJiBunAddress = GetLogDef.stripSpecharsForText(strJiBunAddress)

            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strJiBunAddress] >>" + str(strJiBunAddress))

            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strRoadName] >>" + str(strRoadName))
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strLongitude] >>" + str(strLongitude))
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strLatitude] >>" + str(strLatitude))
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[nProcessStep] >>" + str(nProcessStep))
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[nSequence] >>" + str(nSequence))
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strJiBunAddress] >>" + str(strJiBunAddress))
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strJiBunAddress] >>" + str(strJiBunAddress))



            # qryUpdateGeoPosition = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionBackupTable + " SET "
            qryUpdateGeoPosition = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionCompleteTable + " SET "
            qryUpdateGeoPosition += " text_address = %s, "
            qryUpdateGeoPosition += " road_name = %s, "
            qryUpdateGeoPosition += " longitude = %s, "
            qryUpdateGeoPosition += " latitude = %s, "
            qryUpdateGeoPosition += " geo_point = ST_GeomFromText('POINT("+strLongitude+" "+strLatitude+")'), "
            qryUpdateGeoPosition += " process_step = %s, "
            qryUpdateGeoPosition += " dongmyun_code = %s "
            qryUpdateGeoPosition += " WHERE seq = %s "

            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[qryUpdateGeoPosition] >>" + str(qryUpdateGeoPosition))

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                    inspect.getframeinfo(inspect.currentframe()).lineno) + "qryUpdateGeoPosition>" + str(qryUpdateGeoPosition))

            cursorRealEstate.execute(qryUpdateGeoPosition, (strJiBunAddress, strRoadName, strLongitude, strLatitude, nProcessStep, strDongmyunCode,nSequence))
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
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[ValueError - v] >>" + str(v))
        err_msg = traceback.format_exc(v)
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[ValueError - err_msg] >>" + str(err_msg))

    except QuitException as e:

        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[QuitException - err_msg] >>")
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[ValueError - e] >>" + str(e))

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        err_msg = traceback.format_exc()
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[ValueError - err_msg] >>" + str(err_msg))

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Exception - err_msg] >>")
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[ValueError - e] >>" + str(e))

        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        err_msg = traceback.format_exc()
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[ValueError - err_msg] >>" + str(err_msg))

    else:
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SUCCESS => ============================================================ >>")

    finally:
        logging.info(
            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "Finally END => ============================================================")

if __name__ == '__main__':
    main()