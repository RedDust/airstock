




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
        print(GetLogDef.lineno(__file__), "법원경매 통계 데이터 작성")

        strProcessType = '040201'
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






            # 'curl -v -X POST "https://kauth.kakao.com/oauth/token" \
            #  -H "Content-Type: application/x-www-form-urlencoded" \
            #  -d "grant_type=authorization_code" \
            #  -d "client_id=e68350f56ddf2d5c7bf17964098301d9" \
            #  --data-urlencode "redirect_uri=${REDIRECT_URI}" \
            #  -d "code=${AUTHORIZE_CODE}"'


        strActionUrl = "https://kauth.kakao.com/oauth/authorize"

        headers = {
            'Authorization': 'Bearer e68350f56ddf2d5c7bf17964098301d9',
        }

        data = {
            "template_object" : {
                "object_type": "text",
                "text": "텍스트 영역입니다. 최대 200자 표시 가능합니다.",
                "link": {
                    "web_url": "http://www.landmania.co.kr",
                    "mobile_web_url": "http://www.landmania.co.kr"
                },
                "button_title": "바로 확인"
            },
        }

        header = 'curl -X POST "https://kapi.kakao.com/v2/api/talk/memo/default/send" \
	-H "Content-Type: application/x-www-form-urlencoded" \
	-H "Authorization: Bearer oL9t_CBOTCsXshm0aA_8XXO596VK_GgeqZYKPXObAAABjBlcl6OoblpFv_zasg" \
	-d "template_object=%7B%22object_type%22%3A%22text%22%2C%22text%22%3A%22sdfsdfasdfasdf%22%2C%22link%22%3A%7B%22web_url%22%3A%22http%3A%2F%2Fwww%22%7D%7D"'

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[CRONTAB END]==================================================================")

        strActionUrl = "https://kapi.kakao.com/v2/api/talk/memo/default/send"

        response = requests.post(
            strActionUrl,
            headers=headers,
            data=data,
        )


        print(response.text)



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

