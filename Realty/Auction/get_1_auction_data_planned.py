# 동별로 수집시 IP 차단당함.
# 시군구단위로 수집 해야함. - 20231110
#
import math

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

        strProcessType = '022000'
        strSidoCode = '00'
        strPageNo = '0'
        targetRow = '00'
        strAddressSiguSequence = '0'
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
            strSidoCode = str(rstResult.get('data_1'))

        if strResult == '40':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + '경매 서비스 점검 ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = strSidoCode
        dictSwitchData['data_2'] = strSidoCode
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


        # 초기 값

        # quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴
        #

        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        qrySelectSeoulTradeMaster = "SELECT sido_cd, sido_nm, count(*) as SidoCount FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
        qrySelectSeoulTradeMaster += " WHERE state='00' "
        qrySelectSeoulTradeMaster += " AND sido_cd >= %s"
        qrySelectSeoulTradeMaster += " group by sido_cd"
        qrySelectSeoulTradeMaster += " ORDER BY sido_cd ASC "
        # qrySelectSeoulTradeMaster += " LIMIT 1 "

        cursorRealEstate.execute(qrySelectSeoulTradeMaster , (strSidoCode))
        rstSiDoLists = cursorRealEstate.fetchall()

        # for CityKey, CityValue in AuctionCourtInfo.dictCityPlace.items():
        for rstSiDoList in rstSiDoLists:
            # CityKey = str(rstSiDoList.get('sido_code'))
            # strSiGuCode = str(rstSiDoList.get('sigu_code'))
            strAddressSiguSequence = str(rstSiDoList.get('seq'))
            strSidoCode = str(rstSiDoList.get('sido_cd'))
            strSidoName = str(rstSiDoList.get('sido_nm'))

            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + "rstSiDoList===============================================")
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strSidoCode: (" + str(strSidoCode) + ")")
            logging.info(
                SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strSidoName: (" + str(strSidoName) + ")")

            print("strSidoCode => ", strSidoCode)

            pageNo = 1

            while True:
                strPageNo = str(pageNo)

                print("While True START : pageNo (", strPageNo, ")")
                #
                cookies = AuctionMakeRequestHeader.makePlannedCookies(strSidoCode)
                if cookies == False:
                    raise QuitException("__144cookies__")

                headers = AuctionMakeRequestHeader.makePlannedHeader()
                if headers == False:
                    raise QuitException("__144headers__")

                json_data = AuctionMakeRequestHeader.makePlannedJsonData(strSidoCode, pageNo)

                if json_data == False:
                    raise QuitException("__144json_data__")
                #
                print("cookies => " , cookies)
                print("json_data => ", json_data)
                #
                response = requests.post(
                    'https://www.courtauction.go.kr/pgj/pgjsearch/searchControllerMain.on',
                    cookies=cookies,
                    headers=headers,
                    json=json_data,
                )



                html = response.text  # page_source 얻기

                print("html", html)

                dictGetDatas = json.loads(html)

                if dictGetDatas['status'] != 200:
                    raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴

                dictGetDatas = dictGetDatas['data']

                print("202dictGetDatas => ", type(dictGetDatas), dictGetDatas)

                strIpCheckErrorKeyWord = 'ipcheck'

                if strIpCheckErrorKeyWord in dictGetDatas:
                    if dictGetDatas[strIpCheckErrorKeyWord] == False:
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strIpCheckErrorKeyWord: (" + str(strIpCheckErrorKeyWord) + ")")
                        dictSwitchData = dict()
                        dictSwitchData['result'] = '50'
                        dictSwitchData['data_1'] = strSidoCode
                        dictSwitchData['data_4'] = strPageNo
                        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

                        time.sleep(random.randrange(600, 1200))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strIpCheckErrorKeyWord: (" + str(strIpCheckErrorKeyWord) + ")")
                        continue

                listDltSrchResults = dictGetDatas['dlt_srchResult']

                # print("listDltSrchResults", type(listDltSrchResults), listDltSrchResults)

                intListDltSrchResults = len(listDltSrchResults)
                # print("[intListDltSrchResults] " , intListDltSrchResults)
                # time.sleep(3)
                # quit(500)

                if intListDltSrchResults < 1:
                    print("pageNo", type(pageNo), pageNo)
                    print("listDltSrchResults", type(listDltSrchResults), listDltSrchResults)
                    print("[BREAK] =================================================================")
                    time.sleep(3)
                    break

                for dictDltSrchResults in listDltSrchResults:

                    # print("[dictDltSrchResults]===========================================================")
                    # print("dictDltSrchResults.items()" , len(dictDltSrchResults) , type(dictDltSrchResults) ,  dictDltSrchResults)
                    #
                    # print("dictDltSrchResults.items()['docid']", len(dictDltSrchResults['docid']), dictDltSrchResults['docid'])
                    # print("dictDltSrchResults.items()['hjguSido']", len(dictDltSrchResults['hjguSido']), dictDltSrchResults['hjguSido'])
                    # print("dictDltSrchResults.items()['hjguSigu']", len(dictDltSrchResults['hjguSigu']), dictDltSrchResults['hjguSigu'])
                    #
                    # print("dictDltSrchResults.items()['daepyoSidoCd']", len(dictDltSrchResults['daepyoSidoCd']), dictDltSrchResults['daepyoSidoCd'])
                    # print("dictDltSrchResults.items()['daepyoSiguCd']", len(dictDltSrchResults['daepyoSiguCd']),dictDltSrchResults['daepyoSiguCd'])

                    strDocCD = str(dictDltSrchResults['docid']).strip()
                    strUniqueValue2Enc = str(dictDltSrchResults['daepyoSidoCd']).strip()
                    strAuctionSeq = str(dictDltSrchResults['daepyoSiguCd']).strip()
                    srnSaNo = str(dictDltSrchResults['srnSaNo']).strip()
                    strSidoName = str(dictDltSrchResults['hjguSido']).strip()
                    strSiguName = str(dictDltSrchResults['hjguSigu']).strip()
                    strMaeGiil = str(dictDltSrchResults['maeGiil']).strip()
                    strMaegyuljGiil = str(dictDltSrchResults['maegyuljGiil']).strip()
                    strBoCd = str(dictDltSrchResults['boCd']).strip()

                    strJsonDataRow = str(dictDltSrchResults).strip()
                    json_string = json.dumps(strJsonDataRow)

                    sqlInsertCourtAuctionSpool = " INSERT INTO " + ConstRealEstateTable_AUC.CourtAuctionPlannedSpoolTable + " SET "
                    sqlInsertCourtAuctionSpool += " unique_key= %s,  "
                    sqlInsertCourtAuctionSpool += " boCd= %s,  "
                    sqlInsertCourtAuctionSpool += " maeGiil= %s,  "
                    sqlInsertCourtAuctionSpool += " maegyuljGiil= %s,  "
                    sqlInsertCourtAuctionSpool += " srn_sano= %s,  "
                    sqlInsertCourtAuctionSpool += " sido_code= %s , "
                    sqlInsertCourtAuctionSpool += " sido_name= %s , "
                    sqlInsertCourtAuctionSpool += " sigu_code= %s , "
                    sqlInsertCourtAuctionSpool += " sigu_name= %s , "
                    sqlInsertCourtAuctionSpool += " json_data_row= %s "
                    # logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                    #              "[sqlInsertCourtAuctionSpool: (" + str(len(sqlInsertCourtAuctionSpool)) + ")" + str(
                    #     sqlInsertCourtAuctionSpool))

                    cursorRealEstate.execute(sqlInsertCourtAuctionSpool,[strDocCD,strBoCd,strMaeGiil,strMaegyuljGiil,srnSaNo, strUniqueValue2Enc,strSidoName,strAuctionSeq,strSiguName,json_string])
                    ResRealEstateConnection.commit()
                    nConversionSequence = cursorRealEstate.lastrowid
                print("While True: END pageNo (", pageNo, "), strSidoName (", strSidoName, ")" )
                pageNo += 1
                
                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = strSidoCode
                dictSwitchData['data_2'] = strSidoCode
                dictSwitchData['data_3'] = strSidoName
                dictSwitchData['data_4'] = strPageNo
                LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

                time.sleep(random.randrange(10, 15))
            print("for rstSiDoList in rstSiDoLists: END")



        # END While

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = strSidoCode
        dictSwitchData['data_2'] = strSidoCode
        dictSwitchData['data_3'] = strSidoName
        dictSwitchData['data_4'] = strPageNo
        dictSwitchData['data_5'] = nConversionSequence
        dictSwitchData['data_6'] = strSidoName
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        return True

    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if strSidoCode is not None:
            dictSwitchData['data_1'] = strSidoCode

        if strSidoCode is not None:
            dictSwitchData['data_2'] = strSidoCode

        if strSidoName is not None:
            dictSwitchData['data_3'] = strSidoName

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
