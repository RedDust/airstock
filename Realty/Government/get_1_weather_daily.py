# 날씨 정보를 수집 하는 프로그램
# 정부 커넥션이 너무 불안해서 try~catch 위치를 조정 (실패해도 다음 지역 처리 할 수 있게)
# 040101

import requests


# This is a sample Python script.
import sys
import time
import random
import pymysql
import logging
import logging.handlers
import inspect
import traceback
import re
import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from Lib.CustomException.QuitException import QuitException



import datetime
sys.path.append("D:/PythonProjects/airstock")

#from Helper import basic_fnc

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
from Realty.Government.Const import ConstRealEstateTable_GOV

def main():

    print(GetLogDef.lineno(__file__), "============================================================")
    print(GetLogDef.lineno(__file__), "날씨 정보 수집")

    strProcessType = '040101'
    KuIndex = '00'
    CityKey = '00'
    targetRow = '00'
    strAuctionUniqueNumber = '00'
    strAuctionSeq   =   '0'
    jsonIssueNumber = '0'

    dtNow = DateTime.today()

    dtNowYYYY = str(dtNow.year).zfill(4)
    dtNowMM = str(dtNow.month).zfill(2)
    dtNowDD = str(dtNow.day).zfill(2)

    dtNowHH = str(dtNow.hour).zfill(2)
    dtNowII = str(dtNow.minute).zfill(2)

    dtSearchYYYYMMDD = dtNowYYYY + dtNowMM + dtNowDD
    dtSearchHHII = dtNowHH + dtNowII

    logFileName = dtSearchYYYYMMDD + ".log"

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')

    streamingHandler = logging.StreamHandler()
    streamingHandler.setFormatter(formatter)

    # RotatingFileHandler
    log_max_size = 10 * 1024 * 1024  ## 10MB
    log_file_count = 20

    # RotatingFileHandler
    timeFileHandler = logging.handlers.TimedRotatingFileHandler(
        filename='D:/PythonProjects/airstock/Shell/logs/'+strProcessType+ '_get_auction_' + logFileName,
        when='midnight',
        interval=1,
        encoding='utf-8'
    )
    timeFileHandler.setFormatter(formatter)
    logger.addHandler(streamingHandler)
    logger.addHandler(timeFileHandler)


    # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
    rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
    strResult = rstResult.get('result')
    if strResult is False:
        quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴

    if strResult == '10':
        quit(GetLogDef.lineno(__file__) + 'It is currently in operation. => ' + strResult)  # 예외를 발생시킴

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '10'
    dictSwitchData['data_1'] = KuIndex
    dictSwitchData['data_2'] = CityKey
    dictSwitchData['data_3'] = targetRow
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 "[CRONTAB START]==================================================================")

    # key[58/124] = 서울.금천구

    # key[58/125] = 서울.구로구
    # key[59/125] = 서울.동작구, 서울.관악구
    # key[61/125] = 서울.서초구

    # key[58/126] = 서울.양천구, 서울.강서구 , 서울.영등포구
    # key[60/126] = 서울.용산구
    # key[61/126] = 서울.강남구
    # key[62/126] = 서울.광진구, 서울.송파구, 서울.강동구

    # key[59/127] = 서울.은평구, 서울.서대문구, 서울.마포구
    # key[60/127] = 서울.중구, 서울.종로구
    # key[61/127] = 서울.성동구, 서울.동대문구 , 서울.성북구
    # key[62/127] = 서울.중랑구

    # key[61/128] = 서울.강북구

    # key[61/129] = 서울.도봉구, 서울.노원구

    dictGeoPosition = dict()
    dictGeoPosition['124'] = dict()
    dictGeoPosition['124'][0] = str(58)

    dictGeoPosition['125'] = dict()
    dictGeoPosition['125'][0] = str(58)
    dictGeoPosition['125'][1] = str(59)
    dictGeoPosition['125'][2] = str(61)

    dictGeoPosition['126'] = dict()
    dictGeoPosition['126'][0] = str(58)
    dictGeoPosition['126'][1] = str(60)
    dictGeoPosition['126'][2] = str(61)
    dictGeoPosition['126'][3] = str(62)

    dictGeoPosition['127'] = dict()
    dictGeoPosition['127'][0] = str(59)
    dictGeoPosition['127'][1] = str(60)
    dictGeoPosition['127'][2] = str(61)
    dictGeoPosition['127'][3] = str(62)

    dictGeoPosition['128'] = dict()
    dictGeoPosition['128'][0] = str(61)

    dictGeoPosition['129'] = dict()
    dictGeoPosition['129'][0] = str(61)


    for GeoPositionY, DictGeoPositionValue in dictGeoPosition.items():

        print(GeoPositionY)

        for nGeoLoop, GeoPositionX in DictGeoPositionValue.items():
            print(GeoPositionX)

            try:

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[PROCESS START]["+GeoPositionX+"/"+GeoPositionY+"]===================================")

                strCourtAuctionUrl = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
                strApiKeyEncoded = "1nCBmsCNpq4YULa06T2SMM0T83tFKNUKx0GR7xBHBHCmgv%2B7oN6RodERpyHPeKFnSln48VRpnOUr63EeUl1XFA%3D%3D"
                strApiKeyDecoded = "1nCBmsCNpq4YULa06T2SMM0T83tFKNUKx0GR7xBHBHCmgv+7oN6RodERpyHPeKFnSln48VRpnOUr63EeUl1XFA=="
                # "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst?serviceKey=1nCBmsCNpq4YULa06T2SMM0T83tFKNUKx0GR7xBHBHCmgv%2B7oN6RodERpyHPeKFnSln48VRpnOUr63EeUl1XFA%3D%3D&numOfRows=10&pageNo=1&base_date=20231128&base_time=1300&nx=55&ny=127"

                # 126.92941944444445
                # 37.47877777777778
                listBaseTimes = ['0200', '0500', '0800', '1100', '1400', '1700', '2000', '2300']

                listBaseTimeLoop = 0
                for listBaseTime in listBaseTimes:

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[dtSearchHHII][" + dtSearchHHII + "</>" + listBaseTime + "]")


                    if int(dtSearchHHII) < int(listBaseTime):
                        print("현재시간 보다 기존시간이 크면 이전 시간을 사용한다.")
                        break

                    base_time = listBaseTime
                    listBaseTimeLoop += 1

                if listBaseTimeLoop < 1:
                    nbaseDate = dtNow - TimeDelta(days=1)
                    dtBeforeYYYY = str(nbaseDate.year).zfill(4)
                    dtBeforeMM = str(nbaseDate.month).zfill(2)
                    dtBeforeDD = str(nbaseDate.day).zfill(2)
                    dtSearchYYYYMMDD = dtBeforeYYYY + dtBeforeMM + dtBeforeDD
                    base_time = str('2300')

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[base_datatime]["+dtSearchYYYYMMDD+"/"+base_time+"]")

                # if dtSearchHHII not in listBaseTimes:
                #     nbaseDate = dtNow - TimeDelta(days=1)
                #     dtBeforeYYYY = str(nbaseDate.year).zfill(4)
                #     dtBeforeMM = str(nbaseDate.month).zfill(2)
                #     dtBeforeDD = str(nbaseDate.day).zfill(2)
                #     dtSearchYYYYMMDD = dtBeforeYYYY + dtBeforeMM + dtBeforeDD
                #     base_time = str('2300')
                # else:
                #     base_time = str(dtSearchHHII)

                listRequestData = {
                    'serviceKey': strApiKeyDecoded,
                    'numOfRows': 1000,
                    # 'pageNo': 1000,
                    'dataType': 'JSON',
                    'base_date': dtSearchYYYYMMDD,
                    'base_time': base_time,
                    'nx': GeoPositionX,
                    'ny': GeoPositionY,
                    # 'tmFc': '202311290600',
                    # 'stnId': '108',
                }
                print(strCourtAuctionUrl)
                print(listRequestData)
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[strCourtAuctionUrl]["+str(strCourtAuctionUrl)+"]===================================")

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[listRequestData]["+str(listRequestData)+"]===================================")

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[GeoPositionY]["+str(GeoPositionY)+"][GeoPositionX]["+str(GeoPositionX)+"]")


                response = requests.get(
                    strCourtAuctionUrl,
                    params=listRequestData,
                )



                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[response]["+str(response)+"]===================================")

                responseContents = response.text  # page_source 얻기

                json_object = json.loads(responseContents)
                bMore = json_object.get('response')
                resultCode = str(bMore.get('header').get('resultCode'))
                if resultCode != '00':
                    raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                                  inspect.getframeinfo(inspect.currentframe()).lineno),
                                                'resultCode => ' + str(resultCode))  # 예외를 발생시킴

                objItems = bMore.get('body').get('items').get('item')

                nLoop = 0
                listWeatherKey = []
                dictGetWeatherData = dict()

                #Make Insert Data
                for objItem in objItems:
                    fcstDate = str(objItem.get('fcstDate'))
                    fcstTime = str(objItem.get('fcstTime'))
                    nx = str(objItem.get('nx'))
                    ny = str(objItem.get('ny'))
                    category = str(objItem.get('category'))
                    fcstValue = str(objItem.get('fcstValue'))

                    uniqueValue = nx+ny+fcstDate+fcstTime

                    if uniqueValue not in listWeatherKey:
                        dictGetWeatherData[uniqueValue] = dict()
                        dictGetWeatherData[uniqueValue]['fcstDate'] = fcstDate
                        dictGetWeatherData[uniqueValue]['fcstTime'] = fcstTime
                        dictGetWeatherData[uniqueValue]['nx'] = nx
                        dictGetWeatherData[uniqueValue]['ny'] = ny

                    dictGetWeatherData[uniqueValue][category] = str(fcstValue)

                    listWeatherKey.append(uniqueValue)
                    listWeatherKey = list(set(listWeatherKey))
                    nLoop += 1

                nInsertLoop = 0
                for GetWeatherDataKey , GetWeatherDataValue in dictGetWeatherData.items():
                    if len(GetWeatherDataValue) != 16:
                        continue
                    print(GetWeatherDataKey, GetWeatherDataValue)

                    fcstDate = GetWeatherDataValue.get('fcstDate')
                    fcstTime = GetWeatherDataValue.get('fcstTime')
                    nx = GetWeatherDataValue.get('nx')
                    ny = GetWeatherDataValue.get('ny')
                    POP = str(GetWeatherDataValue.get('POP'))
                    PTY = str(GetWeatherDataValue.get('PTY'))
                    PCP = str(GetWeatherDataValue.get('PCP'))
                    REH = str(GetWeatherDataValue.get('REH'))
                    SNO = str(GetWeatherDataValue.get('SNO'))
                    SKY = str(GetWeatherDataValue.get('SKY'))
                    TMP = str(GetWeatherDataValue.get('TMP'))
                    TMN = str(GetWeatherDataValue.get('TMN'))
                    TMX = str(GetWeatherDataValue.get('TMX'))
                    UUU = str(GetWeatherDataValue.get('UUU'))
                    VVV = str(GetWeatherDataValue.get('VVV'))
                    WAV = str(GetWeatherDataValue.get('WAV'))
                    VEC = str(GetWeatherDataValue.get('VEC'))
                    WSD = str(GetWeatherDataValue.get('WSD'))

                    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
                    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


                    sqlSelectWeather = "SELECT * from "+ ConstRealEstateTable_GOV.WeatherDataTable
                    sqlSelectWeather += " WHERE unique_value = %s "
                    cursorRealEstate.execute(sqlSelectWeather, (GetWeatherDataKey))
                    nResultCount = cursorRealEstate.rowcount
                    if nResultCount > 0:
                        "UPDATE"
                        sqlUpdateWeather = "UPDATE " + ConstRealEstateTable_GOV.WeatherDataTable + " SET "
                        sqlUpdateWeather += " POP = %s , PTY=%s , PCP=%s , REH=%s , SNO=%s,  "
                        sqlUpdateWeather += " SKY = %s , TMP=%s , TMN=%s , TMX=%s , UUU=%s,  "
                        sqlUpdateWeather += " VVV = %s , WAV=%s , VEC=%s , WSD=%s,  "
                        sqlUpdateWeather += " YYYYMMDD = %s, HHII = %s , "
                        sqlUpdateWeather += " modify_date = NOW() "
                        sqlUpdateWeather += " WHERE unique_value = %s "
                        cursorRealEstate.execute(sqlUpdateWeather, (POP,PTY,PCP,REH,SNO, SKY,TMP,TMN,TMX,UUU, VVV,WAV,VEC,WSD,dtSearchYYYYMMDD,dtSearchHHII,GetWeatherDataKey))
                        intAffectedCount = cursorRealEstate.rowcount
                    else:

                        sqlInsertWeather = "INSERT INTO " +  ConstRealEstateTable_GOV.WeatherDataTable + " SET "
                        sqlInsertWeather += " POP = %s , PTY=%s , PCP=%s , REH=%s , SNO=%s,  "
                        sqlInsertWeather += " SKY = %s , TMP=%s , TMN=%s , TMX=%s , UUU=%s,  "
                        sqlInsertWeather += " VVV = %s , WAV=%s , VEC=%s , WSD=%s,  "
                        sqlInsertWeather += "unique_value = % s, YYYYMMDD= %s, HHII= %s,"
                        sqlInsertWeather += "fcstDate = % s, fcstTime= %s, nx= %s, ny = %s"
                        cursorRealEstate.execute(sqlInsertWeather, (POP, PTY, PCP, REH, SNO, SKY, TMP, TMN, TMX, UUU, VVV, WAV, VEC, WSD, GetWeatherDataKey,dtSearchYYYYMMDD,dtSearchHHII,fcstDate,fcstTime,nx,ny))
                        intAffectedCount = cursorRealEstate.rowcount

                    #
                    # if intAffectedCount < 1:
                    #         ResRealEstateConnection.rollback()
                    #         raise QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                                               inspect.getframeinfo(inspect.currentframe()).lineno),
                    #                             'intAffectedCount => ' + str(intAffectedCount))  # 예외를 발생시킴

                    ResRealEstateConnection.commit()
                    ResRealEstateConnection.close()
                    nInsertLoop += 1

                    dictSwitchData = dict()
                    dictSwitchData['result'] = '10'
                    dictSwitchData['data_1'] = nInsertLoop
                    dictSwitchData['data_2'] = GeoPositionY
                    dictSwitchData['data_3'] = GeoPositionX
                    dictSwitchData['data_4'] = GetWeatherDataKey
                    dictSwitchData['data_5'] = dtSearchYYYYMMDD
                    dictSwitchData['data_6'] = dtSearchHHII
                    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[PROCESS END]["+GeoPositionX+"/"+GeoPositionY+"]===================================")



            except QuitException as e:

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '20'
                LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

                err_msg = traceback.format_exc()

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "Error Exception")
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             str(e))
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             str(err_msg))

            except Exception as e:

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '30'
                LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

                err_msg = traceback.format_exc()

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "Error Exception")
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             str(e))
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             str(err_msg))

            else:
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[SUCCESS END]==================================================================")

            finally:
                time.sleep(10)



    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 "[CRON END]==================================================================")

