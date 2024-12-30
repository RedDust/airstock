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
import Realty.Government.MolitLib.GetRoadNameJuso as GetRoadNameJuso



def main():

    try:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "============================================================")



        strProcessType = '100000'
        KuIndex = '00'
        CityKey = '00'
        targetRow = '00'

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
            filename='D:/PythonProjects/airstock/Shell/logs/'+strProcessType+ '_get_temp_test' + logFileName,
            when='midnight',
            interval=1,
            encoding='utf-8'
        )
        timeFileHandler.setFormatter(formatter)
        logger.addHandler(streamingHandler)
        logger.addHandler(timeFileHandler)




        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        #CourtAuctionDataTable
        qrySelectCourtAuctionMaster = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionBackupTable + " WHERE "
        qrySelectCourtAuctionMaster += " dongmyun_code = '00000' AND address_data_text !='' "  # 데이터 있음
        qrySelectCourtAuctionMaster += " ORDER BY seq DESC "  # 데이터 있음
        qrySelectCourtAuctionMaster += " LIMIT 50000 "  # 데이터 있음

        cursorRealEstate.execute(qrySelectCourtAuctionMaster)
        rstFieldsLists = cursorRealEstate.fetchall()

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "qrySelectCourtAuctionMaster =>",
              qrySelectCourtAuctionMaster)

        for SelectColumnList in rstFieldsLists:

            print("====================================================================================")

            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                         inspect.getframeinfo(inspect.currentframe()).lineno), "SelectColumnList =>", SelectColumnList)

            nSequence = SelectColumnList.get('seq')
            strIssueNumber = SelectColumnList.get('issue_number')
            strDBTextAddress = SelectColumnList.get('address_data_text')


            if len(strDBTextAddress) > 0:
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "nSequence =>", nSequence)

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                        inspect.getframeinfo(inspect.currentframe()).lineno), "strDBTextAddress =>", strDBTextAddress)


                dictConversionAddress = GetRoadNameJuso.GetDictConversionAddress(logging, strIssueNumber,strDBTextAddress)

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno), "dictConversionAddress => " , dictConversionAddress)

                admCd = dictConversionAddress['admCd']
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno),"admCd => ", admCd)

                strDongmyunCode = admCd[5:10]

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno),"strDongmyunCode => ", strDongmyunCode)


                qryUpdateGeoPosition = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionBackupTable + " SET "
                # qryUpdateGeoPosition = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " SET "
                qryUpdateGeoPosition += " dongmyun_code = %s "
                qryUpdateGeoPosition += " WHERE seq = %s "

                print("qryUpdateGeoPosition", qryUpdateGeoPosition, type(qryUpdateGeoPosition))
                print("data",strDongmyunCode, nSequence)

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                        inspect.getframeinfo(inspect.currentframe()).lineno) + "qryUpdateGeoPosition>" + str(qryUpdateGeoPosition))

                cursorRealEstate.execute(qryUpdateGeoPosition, (strDongmyunCode,nSequence))
                ResRealEstateConnection.commit()

                # 테스트 딜레이 추가
                if int(nSequence) % 6 == 0:
                    nRandomSec = random.randint(1, 2)
                    print(GetLogDef.lineno(), "Sleep! " + str(nRandomSec) + " Sec!")
                    time.sleep(nRandomSec)






        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                                inspect.getframeinfo(inspect.currentframe()).lineno), "nSequence =>", nSequence)

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
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


