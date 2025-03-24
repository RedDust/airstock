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

def main():
    try:

        # https://curlconverter.com/ <- 프로그램 컨버터

        # 물건상세 검색
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf"

        # 매각예정물건
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveMgakPlanMulSrch.laf"

        # 매각결과
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMgakGyulgwaMulList.laf"

        strProcessType = '021000'

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

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()),
                     "[CRONTAB START]============================================================")

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
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "process_start_date >> " + process_start_date)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "dtRegNow >> " + dtRegNow)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "last_date >> " + last_date)
                quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + strResult)  # 예외를 발생시킴


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'It is currently in operation. => ' + str(
                strResult))  # 예외를 발생시킴

        if strResult == '20':
            data_1 = strAddressSiguSequence = str(rstResult.get('data_1'))

        if strResult == '40':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + '경매 서비스 점검 ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


        # 초기 값

        # quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴
        #

        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionSpoolTable
        qrySelectSeoulTradeMaster += " WHERE state='00' "
        qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
        qrySelectSeoulTradeMaster += " LIMIT 10 "

        cursorRealEstate.execute(qrySelectSeoulTradeMaster)
        rstSpoolDatas = cursorRealEstate.fetchall()

        print("rstSiDoLists =>" , qrySelectSeoulTradeMaster)


        intProcessLoop = 0
        for rstSpoolData in rstSpoolDatas:
            # CityKey = str(rstSiDoList.get('sido_code'))
            # strSiGuCode = str(rstSiDoList.get('sigu_code'))
            data_1 = strAddressSiguSequence = str(rstSpoolData.get('seq'))
            data_2 = strUniqueKey = str(rstSpoolData.get('unique_key'))
            data_3 = strSrnSano = str(rstSpoolData.get('srn_sano'))
            data_4 = strSidoName = str(rstSpoolData.get('sido_name'))
            data_5 = strSiguName = str(rstSpoolData.get('sigu_name'))
            strJsonDataRow= str(rstSpoolData.get('json_data_row'))

            print("strJsonDataRow =>", strJsonDataRow)



            intProcessLoop += 1

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = data_1
            dictSwitchData['data_2'] = data_2
            dictSwitchData['data_3'] = data_3
            dictSwitchData['data_4'] = data_4
            dictSwitchData['data_5'] = data_5
            dictSwitchData['data_6'] = intProcessLoop
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            time.sleep(1)
        print("for rstSiDoList in rstSiDoLists: END")



        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = data_1
        dictSwitchData['data_2'] = data_2
        dictSwitchData['data_3'] = data_3
        dictSwitchData['data_4'] = data_4
        dictSwitchData['data_5'] = data_5
        dictSwitchData['data_6'] = intProcessLoop
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        return True

    except Exception as e:

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


if __name__ == '__main__':
    main()
