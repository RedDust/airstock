#경매 정보 도시구동 Select 가져 오기
#cURL 동작 안해서 실패함 시간 아까움

import requests

# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import datetime
sys.path.append("D:/PythonProjects/airstock")
import logging
import logging.handlers
import inspect
import traceback


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
from Lib.Logging import Logging

def main():

    try:
        print(GetLogDef.lineno(__file__), "============================================================")

        strProcessType = '022001'
        CityKey = '00'
        CityValue = '00'
        strSiGuCode = '00'
        strSiGuName = '00'

        dtNow = DateTime.today()

        Logging.logInit(strProcessType)


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
            quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴

        if strResult == '10':
            quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴

        if strResult == '20':
            quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴

        # if strResult == '30':
        #     quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = CityKey
        dictSwitchData['data_2'] = CityValue
        dictSwitchData['data_3'] = strSiGuCode
        dictSwitchData['data_4'] = strSiGuName
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveAucSigu.ajax"

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[PROCESS START]==================================================================")

        for CityKey, CityValue in AuctionCourtInfo.dictCityPlace.items():

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[CityKey: (" + str(len(CityKey)) + ")" + str(CityKey))

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[CityValue: (" + str(len(CityValue)) + ")" + str(CityValue))

            cookies = {
                'WMONID' : 'sfXO0bVObsW',
                'mvmPlaceSidoCd':'',
                'mvmPlaceSiguCd':'',
                'rd1Cd': '',
                'rd2Cd': '',
                'realVowel': '35207_45207',
                'roadPlaceSidoCd': '',
                'roadPlaceSiguCd': '',
                'vowelSel': '35207_45207',
                'page': 'default40',
                'realJiwonNm': '%BC%AD%BF%EF%C1%DF%BE%D3%C1%F6%B9%E6%B9%FD%BF%F8',
                'page': 'default40',
                'toMul': '%BC%AD%BF%EF%C1%DF%BE%D3%C1%F6%B9%E6%B9%FD%BF%F8%2C20220130112701%2C1%2C20231031%2CB%5E%BC%AD%BF%EF%C1%DF%BE%D3%C1%F6%B9%E6%B9%FD%BF%F8%2C20200130003605%2C3%2C20231107%2CB%5E%BA%CE%BB%EA%C1%F6%B9%E6%B9%FD%BF%F8%2C20120130023039%2C1%2C20231031%2CB%5E',
                'JSESSIONID': 'kcxf3cXX1m3pQDriaO1Q8i059D3pzLSGKn2T7qKi7kM2a1XKnzX4OSwNRzH8WtCj.amV1c19kb21haW4vYWlzMQ==',
                'daepyoSidoCd': CityKey,
                'daepyoSiguCd': '',
            }

            headers = {
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                'Connection': 'keep-alive',
                'Content-Length': '52',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': 'www.courtauction.go.kr',
                'Origin': 'https://www.courtauction.go.kr',
                'Referer': 'https://www.courtauction.go.kr/RetrieveMainInfo.laf',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Google Chrome";v="117", "Not;A=Brand";v="8", "Chromium";v="117"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',

            }

            data = {
                'index': 'FB',
                'sidoCode': CityKey,
                'id2': 'idSiguCode1',
                'id3': 'idDongCode1',
            }

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[headers: (" + str(len(headers)) + ")" + str(headers))

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[cookies: (" + str(len(cookies)) + ")" + str(cookies))

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[data: (" + str(len(data)) + ")" + str(data))


            response = requests.post(
                strCourtAuctionUrl,
                cookies=cookies,
                headers=headers,
                data=data,
            )

            html = response.text  # page_source 얻기
            responseXml = BeautifulSoup(html)

            # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

            siguGroups = responseXml.find_all('option')
            for siguGroup in siguGroups:

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "-------------------------------------------------------------------------------")

                # print(siguGroup.getText)
                strSiGuCode = str(siguGroup.get('value'))
                if len(strSiGuCode) > 1:

                    strSiGuName = str(siguGroup.get_text()).replace("\n","")

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 r"[strSiDoCode: (" + str(len(CityKey)) + ")" + str(CityKey))

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 r"[strSiDoName: (" + str(len(CityValue)) + ")" + str(CityValue))

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 r"[strSiGuCode: (" + str(len(strSiGuCode)) + ")" + str(strSiGuCode))


                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 r"[strSiGuName: (" + str(len(strSiGuName)) + ")" + str(strSiGuName))


                    sqlCourtAuctionSelect = "SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionAddressSiguTable + " WHERE sido_code = %s AND sigu_code = %s LIMIT 1 "
                    cursorRealEstate.execute(sqlCourtAuctionSelect, (CityKey,strSiGuCode ))
                    nResultCount = cursorRealEstate.rowcount
                    if nResultCount < 1:
                        sqlCourtAuctionInsert = " INSERT INTO " + ConstRealEstateTable_AUC.CourtAuctionAddressSiguTable + " SET "
                        sqlCourtAuctionInsert += " sido_code= %s , "
                        sqlCourtAuctionInsert += " sido_name= %s , "
                        sqlCourtAuctionInsert += " sigu_code= %s , "
                        sqlCourtAuctionInsert += " sigu_name= %s  "
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[sqlCourtAuctionInsert: (" + str(len(sqlCourtAuctionInsert)) + ")" + str(sqlCourtAuctionInsert))

                        cursorRealEstate.execute(sqlCourtAuctionInsert, (CityKey, CityValue, strSiGuCode, strSiGuName))
                        ResRealEstateConnection.commit()

                        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                        dictSwitchData = dict()
                        dictSwitchData['result'] = '10'
                        dictSwitchData['data_1'] = CityKey
                        dictSwitchData['data_2'] = CityValue
                        dictSwitchData['data_3'] = strSiGuCode
                        dictSwitchData['data_4'] = strSiGuName
                        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)



        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = CityKey
        dictSwitchData['data_2'] = CityValue
        dictSwitchData['data_3'] = strSiGuCode
        dictSwitchData['data_4'] = strSiGuName
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)



    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'

        err_msg = traceback.format_exc()
        if CityKey is not None:
            dictSwitchData['data_1'] = CityKey

        if CityValue is not None:
            dictSwitchData['data_2'] = CityValue

        if strSiGuCode is not None:
            dictSwitchData['data_3'] = strSiGuCode

        if strSiGuName is not None:
            dictSwitchData['data_4'] = strSiGuName

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

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
                     "[PROCESS END]==================================================================")

