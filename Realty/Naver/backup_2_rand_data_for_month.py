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
        print(GetLogDef.lineno(__file__), "네이버 물건 월별 백업 및 마스터 삭제")

        strProcessType = '011010'
        KuIndex = '00'
        CityKey = '00'
        targetRow = '00'
        strAuctionUniqueNumber = '00'
        strAuctionSeq   =   '0'
        jsonIssueNumber = '0'

        dtNow = DateTime.today()

        dtResourceTomorrow = dtNow - TimeDelta(days=45)
        dtBackupEndRegDate = str(dtResourceTomorrow.strftime('%Y-%m-%d 00:00:00'))




        nInsertedCount = 0
        nUpdateCount = 0

        logFileName = str(dtNow.year).zfill(4) + str(dtNow.month).zfill(2) + str(dtNow.day).zfill(2) + ".log"

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
            filename='D:/PythonProjects/airstock/Logs/'+strProcessType+ '_naver_backup_' + logFileName,
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
            print("ERROR => strResult", strResult)
            quit(GetLogDef.lineno(__file__) + 'strResult => ' + strResult)  # 예외를 발생시킴

        if strResult == '10':
            print("ERROR => strResult", strResult)
            quit(GetLogDef.lineno(__file__) + 'It is currently in operation. => ' + strResult)  # 예외를 발생시킴

        if strResult == '20':
            print("ERROR => strResult", strResult)
            quit(GetLogDef.lineno(__file__) + 'It is currently in operation. => ' + strResult)  # 예외를 발생시킴

        if strResult == '30':
            print("ERROR => strResult", strResult)
            quit(GetLogDef.lineno(__file__) + 'It is currently in operation. => ' + strResult)  # 예외를 발생시킴



        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[CRONTAB START]==================================================================")

        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        qrySelectSeoulSwitch = " SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable
        qrySelectSeoulSwitch += " WHERE reg_date < %s  "
        qrySelectSeoulSwitch += " ORDER BY seq ASC LIMIT 300000 "
        cursorRealEstate.execute(qrySelectSeoulSwitch, dtBackupEndRegDate)
        rstNaverMasterDataLists = cursorRealEstate.fetchall()
        intSelectRowCount = cursorRealEstate.rowcount


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_4'] = intSelectRowCount
        dictSwitchData['data_6'] = '00'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        print("qrySelectSeoulSwitch", qrySelectSeoulSwitch , dtBackupEndRegDate)

        intTotalProcessCount = 0
        intETCProcessCount = 0
        intBackupCount = 0
        intMinuteCount = 0
        intProcessPerMinute = 0

        for rstNaverMasterDataList in rstNaverMasterDataLists:

            intTotalProcessCount += 1
            intMinuteCount += 1
            dtForNow = DateTime.today()
            dtMinuteRegDate = str(dtForNow.strftime('%S')).zfill(2)

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[dtMinuteRegDate ------------------  ]==>" + dtMinuteRegDate)

            if dtMinuteRegDate == '00' and intMinuteCount > 200:
                # 1분당 처리 되는 건수
                intProcessPerMinute = intMinuteCount
                intMinuteCount = 0


            intNMMTDBMasterSeq = str(rstNaverMasterDataList.get('seq'))
            strNMMTDBAtclCfmYmd = str(rstNaverMasterDataList.get('atclCfmYmd'))
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[intNMMTDBMasterSeq START ########################################### ]==>" + intNMMTDBMasterSeq )

            print("---------------------------------  strNMMTDBAtclCfmYmd >> " + strNMMTDBAtclCfmYmd + " | intNMMTDBMasterSeq >>" + intNMMTDBMasterSeq )


            if len(strNMMTDBAtclCfmYmd) < 8:

                intETCProcessCount += 1

                strBackupTableName = "kt_realty_naver_mobile_backup_ETC"

                # Backup to Backup Table
                qryInsertBackupTable = "INSERT INTO " + strBackupTableName + " SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + " WHERE seq = %s "
                cursorRealEstate.execute(qryInsertBackupTable, intNMMTDBMasterSeq)

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[qryInsertBackupTable]==>" + qryInsertBackupTable)

                # Detele
                qryDeleteMasterTable = " DELETE FROM " + ConstRealEstateTable.NaverMobileMasterTable + " WHERE seq = %s "
                cursorRealEstate.execute(qryDeleteMasterTable, intNMMTDBMasterSeq)

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[qryDeleteMasterTable]==>" + qryDeleteMasterTable)

            else:

                intBackupCount += 1

                listNMMTDBAtclCfmYmds = str(strNMMTDBAtclCfmYmd).split(".")

                strAtclCfmYear = "20"+str(listNMMTDBAtclCfmYmds[0])
                strAtclCfmMonth = str(listNMMTDBAtclCfmYmds[1]).zfill(2)
                strAtclCfmDay = str(listNMMTDBAtclCfmYmds[2]).zfill(2)

                strBackupTableNameYYYYMM =  strAtclCfmYear + strAtclCfmMonth
                strBackupTableName = "kt_realty_naver_mobile_backup_" + strBackupTableNameYYYYMM


                qryCheckTable = "show tables like '" + strBackupTableName + "' "
                cursorRealEstate.execute(qryCheckTable)
                rstShowTableList = cursorRealEstate.fetchall()
                intBackupTableResult = len(rstShowTableList)

                # Create backup table
                if intBackupTableResult < 1:
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[Create backup table]=>" + strBackupTableName )
                    qryCreateBackupTable = "create table " + strBackupTableName + " LIKE " + ConstRealEstateTable.NaverMobileMasterTable
                    cursorRealEstate.execute(qryCreateBackupTable)

                #Backup to Backup Table
                qryInsertBackupTable = "INSERT INTO " + strBackupTableName + " SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + " WHERE seq = %s"
                cursorRealEstate.execute(qryInsertBackupTable, intNMMTDBMasterSeq)

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[qryInsertBackupTable]==>" + qryInsertBackupTable)

                #Detele
                qryDeleteMasterTable = " DELETE FROM " +  ConstRealEstateTable.NaverMobileMasterTable + " WHERE seq = %s "
                cursorRealEstate.execute(qryDeleteMasterTable, intNMMTDBMasterSeq)

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[qryDeleteMasterTable]==>" + qryDeleteMasterTable)


            # ResRealEstateConnection.rollback()
            ResRealEstateConnection.commit()

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = intNMMTDBMasterSeq   # naver Master 테이블의 seq
            dictSwitchData['data_2'] = strBackupTableName   # 백업 대상 테이블
            dictSwitchData['data_3'] = strNMMTDBAtclCfmYmd  # 백업 처리 일
            dictSwitchData['data_5'] = intBackupCount       # 백업 성공 건수
            dictSwitchData['data_6'] = intProcessPerMinute  # 1분당 처리 건수
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[intNMMTDBMasterSeq END ]==>" + intNMMTDBMasterSeq )

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[CRONTAB END]==================================================================")


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


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
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[CRONTAB END]==================================================================")

