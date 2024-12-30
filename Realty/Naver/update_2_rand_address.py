# https://api.ncloud-docs.com/docs/ai-naver-mapsreversegeocoding-gc
# curl "https://naveropenapi.apigw.ntruss.com/map-reversegeocode/v2/gc?coords={입력_좌표}&sourcecrs={좌표계}&orders={변환_작업_이름}&output={출력_형식}" \
# 	-H "X-NCP-APIGW-API-KEY-ID: {애플리케이션 등록 시 발급받은 client id값}" \
# 	-H "X-NCP-APIGW-API-KEY: {애플리케이션 등록 시 발급받은 client secret값}" -v



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
from Lib.GeoDataModule import GeoDataModule

#
# def main():

try:
    print(GetLogDef.lineno(__file__), "============================================================")
    print(GetLogDef.lineno(__file__), "kt_realty_address_conversion 테이블의 keyword 및 주소 업데이트")

    strProcessType = '010101'
    KuIndex = '00'
    CityKey = '00'
    targetRow = '00'
    strAuctionUniqueNumber = '00'
    strAuctionSeq   =   '0'
    jsonIssueNumber = '0'

    dtNow = DateTime.today()
    # print(dtNow.hour)
    # print(dtNow.minute)
    # print(dtNow)

    logFileName = str(dtNow.year) + str(dtNow.month) + str(dtNow.day).zfill(2) + ".log"

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
        filename='D:/PythonProjects/airstock/Shell/logs/'+strProcessType+ '_update_2_rand_address_' + logFileName,
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
        quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴

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

    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
    dtBackupEndRegDate = 1


    qrySelectSeoulSwitch = " SELECT * FROM kt_realty_naver_mobile_backup_202211 "
    qrySelectSeoulSwitch += " WHERE seq >= %s  "
    qrySelectSeoulSwitch += " AND seq >= %s  "
    qrySelectSeoulSwitch += " ORDER BY seq ASC LIMIT 1"
    cursorRealEstate.execute(qrySelectSeoulSwitch, dtBackupEndRegDate)
    rstNaverMasterDataLists = cursorRealEstate.fetchall()
    intSelectRowCount = cursorRealEstate.rowcount
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 "[intSelectRowCount] => " + str(intSelectRowCount))


    for rstNaverMasterDataList in rstNaverMasterDataLists:

        intNMMTDBMasterSeq = str(rstNaverMasterDataList.get('seq'))
        # print("intNMMTDBMasterSeq" , intNMMTDBMasterSeq )
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[intNMMTDBMasterSeq] => " + str(intNMMTDBMasterSeq))

        floatNMMTLat = float(rstNaverMasterDataList.get('lat'))
        floatNMMTLng = float(rstNaverMasterDataList.get('lng'))

        dictReverseGeoData = GeoDataModule.getNaverReverseGeoData(logging, str(floatNMMTLng), str(floatNMMTLat))

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) + "dictReverseGeoData => " , dictReverseGeoData)


        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[intSelectRowCount] => " + str(intSelectRowCount))



























    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 "[CRONTAB END]==================================================================")


    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_1'] = KuIndex
    dictSwitchData['data_2'] = CityKey
    dictSwitchData['data_3'] = targetRow
    dictSwitchData['data_4'] = strAuctionUniqueNumber
    dictSwitchData['data_5'] = strAuctionSeq
    dictSwitchData['data_6'] = jsonIssueNumber
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


except Exception as e:

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '30'

    if KuIndex is not None:
        dictSwitchData['data_1'] = KuIndex

    if CityKey is not None:
        dictSwitchData['data_2'] = CityKey

    if targetRow is not None:
        dictSwitchData['data_3'] = targetRow

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
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 "[CRONTAB END]==================================================================")

