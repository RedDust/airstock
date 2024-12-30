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
from Realty.Auction.AuctionLib import MakeAuctionUniqueKey


def main():

    try:
        print(GetLogDef.lineno(__file__), "============================================================")

        strProcessType = '020201'
        KuIndex = '00'
        CityKey = '00'
        targetRow = '00'
        strAuctionUniqueNumber = '00'
        strAuctionSeq   =   '0'
        jsonIssueNumber = '0'



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









        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        print(GetLogDef.lineno(__file__), "################################################################")
        sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " "
        sqlCourtAuctionSelect += " LIMIT 1000"
        cursorRealEstate.execute(sqlCourtAuctionSelect)
        rstBackupLists = cursorRealEstate.fetchall()
        nLoopCount = 0

        for rstBackupList in rstBackupLists:
            build_type_text = rstBackupList.get('build_type_text')
            build_type_text = re.sub(r"[^가-힣]", "", build_type_text)

            nLoopCount += 1

            # list_build_type_text = str(build_type_text).split(",")
            # intLenListBuileType = len(list_build_type_text)
            # list_build_type_text.pop(0)

            print("nLoopCount > ", nLoopCount , build_type_text )




            # for list_build_type in list_build_type_text:
            #     list_build_type = re.sub(r"[^가-힣]", "", list_build_type)
            #
            #
            # sqlSelectAnalisys  = " SELECT * FROM kt_realty_court_auction_build_type_analysis_temp "
            # sqlSelectAnalisys += " WHERE build_text = %s  "
            # cursorRealEstate.execute(sqlSelectAnalisys, (build_type_text))
            # nResultCount = cursorRealEstate.rowcount
            # if nResultCount > 0:
            #     sqlUpdatetAnalisys = " UPDATE kt_realty_court_auction_build_type_analysis_temp SET "
            #     sqlUpdatetAnalisys += " text_count = text_count + 1 "
            #     sqlUpdatetAnalisys += " WHERE build_text = %s  "
            #     cursorRealEstate.execute(sqlUpdatetAnalisys, (build_type_text))
            # else:
            #     sqlInsertAnalisys = " INSERT INTO kt_realty_court_auction_build_type_analysis_temp SET "
            #     sqlInsertAnalisys += " build_text = %s  "
            #     cursorRealEstate.execute(sqlInsertAnalisys, (build_type_text))
            #
            # ResRealEstateConnection.commit()



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

