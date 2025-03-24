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
import Realty.Auction.AuctionLib.AuctionDataDecode as AuctionDataDecode
import Realty.Government.MolitLib.GetRoadNameJuso as GetRoadNameJuso
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF


def main():

    try:


        return True

        #https://curlconverter.com/ <- 프로그램 컨버터

        # 물건상세 검색
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf"

        # 매각예정물건
        #strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveMgakPlanMulSrch.laf"


        # 매각결과
        #strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMgakGyulgwaMulList.laf"

        strProcessType = '020000'
        KuIndex = '00'
        CityKey = '00'
        targetRow = '00'
        nAddressSiguSequence = '0'
        dtNow = DateTime.today()
        # print(dtNow.hour)
        # print(dtNow.minute)
        # print(dtNow)

        LogPath = 'CourtAuction/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "[CRONTAB START]============================================================")

        # 스위치 데이터 조회 type(000200) result (10:정기점검)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2('000200')
        strResult = rstResult.get('result')
        if strResult is False:
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => '+ str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            process_start_date = rstResult.get('process_start_date').strftime('%Y-%m-%d %H:%M:%S')
            last_date = rstResult.get('last_date').strftime('%Y-%m-%d %H:%M:%S')
            dtRegNow = DateTime.today()
            process_start_date_obj = DateTime.strptime(process_start_date, '%Y-%m-%d %H:%M:%S')
            last_date_obj = DateTime.strptime(last_date, '%Y-%m-%d %H:%M:%S')

            if (process_start_date_obj <= dtRegNow ) and (dtRegNow <= last_date_obj):
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "process_start_date >> "+ process_start_date)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "dtRegNow >> "+ dtRegNow)
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "last_date >> "+ last_date)
                quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => '+ strResult)  # 예외를 발생시킴


        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'It is currently in operation. => '+ str(strResult)) # 예외를 발생시킴

        if strResult == '20':
            nAddressSiguSequence = rstResult.get('data_2')

        if strResult == '40':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + '경매 서비스 점검 '+ str(strResult)) # 예외를 발생시킴


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = KuIndex
        dictSwitchData['data_2'] = nAddressSiguSequence
        dictSwitchData['data_3'] = targetRow
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)



        # 기초 헤더 정리
        # headers = {
        #     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        #     'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        #     'Cache-Control': 'max-age=0',
        #     'Connection': 'keep-alive',
        #     'Content-Type': 'application/x-www-form-urlencoded',
        #     'Origin': 'https://www.courtauction.go.kr',
        #     'Referer': 'https://www.courtauction.go.kr/RetrieveMainInfo.laf',
        #     'Sec-Fetch-Dest': 'document',
        #     'Sec-Fetch-Mode': 'navigate',
        #     'Sec-Fetch-Site': 'same-origin',
        #     'Sec-Fetch-User': '?1',
        #     'Upgrade-Insecure-Requests': '1',
        #     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        #     'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        #     'sec-ch-ua-mobile': '?0',
        #     'sec-ch-ua-platform': '"Windows"',
        # }

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www.courtauction.go.kr',
            'Referer': 'https://www.courtauction.go.kr/RetrieveMainInfo.laf',
            'Sec-Fetch-Dest': 'frame',
        }

        # 초기 값
        paging = 40
        # quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴
        #

        for KuIndex, AuctionCallInfo in AuctionCourtInfo.dictAuctionTypes.items():

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "AuctionCourtInfo.dictAuctionTypes.items()==================================================================")
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +"[KuIndex: (" + str(KuIndex) + ")")
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +"[AuctionCallInfo: (" + str(AuctionCallInfo) + ")")

            strCourtAuctionUrl = AuctionCourtInfo.dictAuctionTypes[KuIndex].get('url')
            strAuctionType = AuctionCourtInfo.dictAuctionTypes[KuIndex].get('type')

            # print(strCourtAuctionUrl)
            # print(strCourtAuctionUrl)

            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

            qrySelectRoad = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionAddressSiguTable

            qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
            qrySelectSeoulTradeMaster += " WHERE state='00' AND sgg_cd<>'000' AND umd_cd='000' AND ri_cd='00'"
            qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
            qrySelectSeoulTradeMaster += " LIMIT 1000 "

            cursorRealEstate.execute(qrySelectSeoulTradeMaster)
            rstSiDoLists = cursorRealEstate.fetchall()


            # for CityKey, CityValue in AuctionCourtInfo.dictCityPlace.items():
            for rstSiDoList in rstSiDoLists:
                # CityKey = str(rstSiDoList.get('sido_code'))
                # strSiGuCode = str(rstSiDoList.get('sigu_code'))
                nAddressSiguSequence = str(rstSiDoList.get('seq'))
                CityKey = str(rstSiDoList.get('sido_cd'))
                strSiGuCode = str(rstSiDoList.get('sgg_cd'))

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "rstSiDoList===============================================")
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[CityKey: (" + str(CityKey) + ")")
                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strSiGuCode: (" + str(strSiGuCode) + ")")

                targetRow = 1


                while True:

                    cookies = {
                        'WMONID': 'MMCPCPmrU9T',
                        'daepyoSiguCd': '',
                        'mvmPlaceSiguCd': '',
                        'rd1Cd': '',
                        'rd2Cd': '',
                        'realVowel': '35207_45207',
                        'roadPlaceSidoCd': '',
                        'roadPlaceSiguCd': '',
                        'vowelSel': '35207_45207',
                        'page': 'default20',
                        'realJiwonNm': '%BC%AD%BF%EF%B3%B2%BA%CE%C1%F6%B9%E6%B9%FD%BF%F8',
                        'daepyoSidoCd': CityKey,
                        'mvmPlaceSidoCd': '',
                        'JSESSIONID': 'GaL7Ehe4mYWbEDHCagGn6L60inn16EKTWsNnMnXVnjQRRAlVbAGe1zK51jnl3BZW.amV1c19kb21haW4vYWlzMg==',
                    }


                    cookies = {
                        'WMONID': 'sfXO0bVObsW',
                        'mvmPlaceSidoCd': '',
                        'mvmPlaceSiguCd': '',
                        'roadPlaceSidoCd': '',
                        'roadPlaceSiguCd': '',
                        'vowelSel': '35207_45207',
                        'realVowel': '35207_45207',
                        'page': 'default40',
                        'rd1Cd': '',
                        'rd2Cd': '',
                        'daepyoSidoCd': CityKey,
                        'JSESSIONID': 'puVVWLfGqX1Yj56jhYabW71ayH98vAGEZEYEWcecBzOn6TR1nWGLZspaWBbtsyPe.amV1c19kb21haW4vYWlzMQ==',
                        'daepyoSiguCd': strSiGuCode,
                    }

                    headers = {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                        'Content-Type': 'application/x-www-form-urlencoded',
                        # 'Cookie': 'WMONID=sfXO0bVObsW; mvmPlaceSidoCd=; mvmPlaceSiguCd=; roadPlaceSidoCd=; roadPlaceSiguCd=; vowelSel=35207_45207; realJiwonNm=^%^BC^%^AD^%^BF^%^EF^%^C1^%^DF^%^BE^%^D3^%^C1^%^F6^%^B9^%^E6^%^B9^%^FD^%^BF^%^F8; realVowel=35207_45207; page=default40; rd1Cd=; rd2Cd=; daepyoSidoCd=11; JSESSIONID=puVVWLfGqX1Yj56jhYabW71ayH98vAGEZEYEWcecBzOn6TR1nWGLZspaWBbtsyPe.amV1c19kb21haW4vYWlzMQ==; daepyoSiguCd=500',
                        'Origin': 'https://www.courtauction.go.kr',
                        'Referer': 'https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf',
                        'Sec-Fetch-Dest': 'frame',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'same-origin',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
                        'sec-ch-ua-mobile': '?0',
                    }

                    data = 'page=default40&page=default40&srnID=PNO102000&jiwonNm=&bubwLocGubun=2&jibhgwanOffMgakPlcGubun=&mvmPlaceSidoCd=&mvmPlaceSiguCd=&roadPlaceSidoCd=&roadPlaceSiguCd=' \
                           '&daepyoSidoCd='+CityKey+'&daepyoSiguCd='+strSiGuCode+'&daepyoDongCd=&rd1Cd=&rd2Cd=&rd3Rd4Cd=&roadCode=&notifyLoc=1&notifyRealRoad=1&notifyNewLoc=1&mvRealGbncd=1' \
                           '&jiwonNm1=^%^C0^%^FC^%^C3^%^BC&jiwonNm2=^%^BC^%^AD^%^BF^%^EF^%^C1^%^DF^%^BE^%^D3^%^C1^%^F6^%^B9^%^E6^%^B9^%^FD^%^BF^%^F8' \
                                                    '&mDaepyoSidoCd='+CityKey+'&mvDaepyoSidoCd=&mDaepyoSiguCd='+strSiGuCode+'' \
                           '&mvDaepyoSiguCd=&realVowel=00000_55203&vowelSel=00000_55203&mDaepyoDongCd=&mvmPlaceDongCd=&_NAVI_CMD=&_NAVI_SRNID=&_SRCH_SRNID=PNO102000&_CUR_CMD=RetrieveMainInfo.laf' \
                           '&_CUR_SRNID=PNO102000&_NEXT_CMD=&_NEXT_SRNID=PNO102002&_PRE_SRNID=PNO102001&_LOGOUT_CHK=&_FORM_YN=Y' \
                           '&PNIPassMsg=^%^C1^%^A4^%^C3^%^A5^%^BF^%^A1+^%^C0^%^C7^%^C7^%^D8+^%^C2^%^F7^%^B4^%^DC^%^B5^%^C8+^%^C7^%^D8^%^BF^%^DCIP+^%^BB^%^E7^%^BF^%^EB^%^C0^%^DA^%^C0^%^D4^%^B4^%^CF^%^B4^%^D9.' \
                           '&pageSpec=default40&pageSpec=default40&targetRow='+str(targetRow)+'&lafjOrderBy=order+by+maeGiil+asc'


                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[data: (" + str(data) + ")")


                    response = requests.post(
                        strCourtAuctionUrl,
                        # cookies=cookies,
                        headers=headers,
                        data=data,
                    )

                    if response is None:
                        time.sleep(2)
                        continue

                    html = response.text  # page_source 얻기
                    soup = BeautifulSoup(html, "html.parser")  # get html

                    # logging.info(
                    #     SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[soup: (" + str(soup) + ")")

                    rstMainElements = soup.select_one('#contents > div.table_contents > form:nth-child(1) > table > tbody')

                    # logging.info(
                    #     SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[rstMainElements: (" + str(rstMainElements) + ")")

                    nLoopTrElements = 0
                    rstTrElements = rstMainElements.select('tr')
                    # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "-------------------------------------------------------------------------------")
                    strErrorMessage = rstMainElements.select_one('tr').select_one('td').text
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strErrorMessage: (" + str(strErrorMessage) + ")")
                    if strErrorMessage == '검색결과가 없습니다.':
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     str(targetRow) + "]검색결과가 없습니다")
                        break

                    for rstTrElement in rstTrElements:
                        nLoopTrElements = nLoopTrElements + 1

                        nLoopTdElements = 0
                        rstTdElements = rstTrElement.select('td')


                        # DB 연결
                        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
                        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


                        # 법원경매 0번째 컬럼 (CHECKBOX - 법정,고유코드,) START
                        strCheckBoxValues = (rstTdElements[0].select_one('input[type=checkbox]').get('value'))
                        arrCheckBoxValues = strCheckBoxValues.split(',')
                        if len(arrCheckBoxValues) != 3:
                            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + ' arrCheckBoxValues => ' + str(arrCheckBoxValues))  # 예외를 발생시킴

                        # print(arrCheckBoxValues)
                        strCourtName = arrCheckBoxValues[0]
                        strAuctionUniqueNumber = arrCheckBoxValues[1]
                        strAuctionSeq = arrCheckBoxValues[2]
                        # 법원경매 0번째 컬럼 (CHECKBOX) END

                        # 법원경매 1번째 컬럼 (사건번호) START
                        rstIssueNumber = rstTdElements[1]
                        rstIssueNumber.find('input').decompose()
                        strIssueNumber = rstIssueNumber.text
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[rstIssueNumber:" + str(rstIssueNumber))

                        nLoopTempIssueNumber = 0
                        arrTempIssueNumber = []
                        arrIssueNumbers = strIssueNumber.split("\n")
                        for arrIssueNumber in arrIssueNumbers:
                            strTempIssueNumber = repr(arrIssueNumber)

                            # strTempIssueNumber = strTempIssueNumber.replace("\\t", "")
                            # strTempIssueNumber = strTempIssueNumber.replace("\'", "")
                            # strTempIssueNumber = strTempIssueNumber.replace(" ", "")
                            # strTempIssueNumber = strTempIssueNumber.replace("\\r", "")

                            strTempIssueNumber = GetLogDef.stripSpecharsForText(strTempIssueNumber)



                            # print(strTempIssueNumber)
                            if len(strTempIssueNumber) > 0:
                                arrTempIssueNumber.append(strTempIssueNumber)
                                strTempIssueNumber = nLoopTempIssueNumber + 1

                        # 법원 정보가 존재 하지 않으면 오류 처리
                        if arrTempIssueNumber[0] not in AuctionCourtInfo.arrCourtName:
                            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'arrTempIssueNumber[0] => '+ str(arrTempIssueNumber[0]))  # 예외를 발생시킴

                        # 사건 번호 정보가 없으면 오류
                        if len(arrTempIssueNumber) < 2:
                            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'len(arrTempIssueNumber) => ' + str(arrTempIssueNumber[0]))  # 예외를 발생시킴


                        # #중복 물건 또는 병합물건 존재함
                        # if len(arrTempIssueNumber) > 2:
                        #     print("어레이가 2개 이상", len(arrTempIssueNumber))

                        jsonIssueNumber = json.dumps(arrTempIssueNumber, ensure_ascii=False)
                        # print(jsonIssueNumber)
                        # jsonIssueNumber
                        # 법원경매 1번째 컬럼 (사건번호) END

                        # 법원경매 2번째 컬럼 (용도번호) START
                        rstUsage = rstTdElements[2]
                        arrUsages = str(rstUsage).split("<br/>")
                        nLoopTempUsageInfo = 0
                        arrTempUsageInfo = []
                        for arrUsage in arrUsages:
                            strTempUsage = repr(arrUsage)
                            strTempUsage = strTempUsage.replace('<td>', '')
                            # strTempUsage = strTempUsage.replace("\'", "")
                            # strTempUsage = strTempUsage.replace("\\n", "")
                            # strTempUsage = strTempUsage.replace("\\t", "")
                            # strTempUsage = strTempUsage.replace("\\r", "")
                            # strTempUsage = strTempUsage.replace(" ", "")

                            strTempUsage = GetLogDef.stripSpecharsForText(strTempUsage)


                            strTempUsage = strTempUsage.replace('</td>', '')
                            if len(strTempUsage) > 0:
                                arrTempUsageInfo.append(strTempUsage)
                                nLoopTempUsageInfo = nLoopTempUsageInfo + 1

                        # 사건 번호 정보가 없으면 오류
                        if len(arrTempUsageInfo) < 2:
                            raise Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'len(arrTempIssueNumber) => ' + str(len(arrTempUsageInfo)))  # 예외를 발생시킴

                        # #중복 물건 또는 병합물건 존재함
                        # if len(arrTempUsageInfo) > 2:
                        #     print("어레이가 2개 이상", len(arrTempUsageInfo))

                        jsonUsageInfo = json.dumps(arrTempUsageInfo, ensure_ascii=False)

                        strBuildTypeText = re.sub(r"[^가-힣]", "", jsonUsageInfo)

                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " AuctionCourtInfo.dictBuildTypeReverseKeyWord[strBuildTypeText] > " + str(AuctionCourtInfo.dictBuildTypeReverseKeyWord[strBuildTypeText]))

                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " AuctionCourtInfo.dictBuildTypeKeyWord[AuctionCourtInfo.dictBuildTypeReverseKeyWord[strBuildTypeText]]> " +
                              str(AuctionCourtInfo.dictBuildTypeToCode[AuctionCourtInfo.dictBuildTypeReverseKeyWord[strBuildTypeText]]))

                        strBuildTypeCode = AuctionCourtInfo.dictBuildTypeToCode[AuctionCourtInfo.dictBuildTypeReverseKeyWord[strBuildTypeText]]

                        # print(jsonUsageInfo)

                        # ["'1'", "'대지'", "'임야'", "'전답'"]
                        # 법원경매 2번째 컬럼 (용도번호) END

                        # 법원경매 3번째 컬럼 (소재지 및 내역) START
                        rstAddressAndContents = rstTdElements[3]
                        # print(rstAddressAndContents)
                        nLoopTempAddressInfo = 0
                        arrTempAddressInfo = []
                        arrayAtags = rstAddressAndContents.select('a')
                        for arrayAtag in arrayAtags:
                            # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()),"-------------------------------------------------------------------------------")
                            strTempAddress = repr(arrayAtag.text)
                            strTempAddress = GetLogDef.stripSpecharsForText(strTempAddress)
                            if len(strTempAddress) < 1:
                                continue

                            arrTempAddressInfo.append(strTempAddress)
                            nLoopTempAddressInfo = nLoopTempAddressInfo + 1

                        jSonAddressInfo = json.dumps(arrTempAddressInfo, ensure_ascii=False)

                        # print(arrTempAddressInfo)
                        # ['서울특별시 관악구 신림동  103-260 ', '서울특별시 관악구  복은6길 20-4 ']
                        # 법원경매 3번째 컬럼 (소재지 및 내역) END


                        # 법원경매 4번째 컬럼 (비고) START
                        rstEtcContents = rstTdElements[4]
                        strTempContents = repr(rstEtcContents.text)
                        strTempContents = GetLogDef.stripSpecharsForText(strTempContents)
                        # print(strTempContents)
                        # 법원경매 4번째 컬럼 (비고) END


                        # 법원경매 5번째 컬럼 (감정평가액 / 최처매각가격) START
                        rstAuctionCosts = rstTdElements[5]
                        rstDivs = rstAuctionCosts.select('div')
                        nLoopTempAddressInfo = 0
                        arrActionCustInfo = []
                        for rstDiv in rstDivs:
                            strTempAuctionCosts = repr(rstDiv.text)
                            strTempAuctionCosts = strTempAuctionCosts.split('\\n')

                            for strAuctionCost in strTempAuctionCosts:
                                strTempAuctionCost = GetLogDef.stripSpecharsForText(strAuctionCost)
                                # strTempAuctionCost = strTempAuctionCost.replace("\\n", "")
                                # strTempAuctionCost = strTempAuctionCost.replace("\\t", "")
                                # strTempAuctionCost = strTempAuctionCost.replace("\\r", "")
                                # strTempAuctionCost = strTempAuctionCost.replace(" ", "")
                                # strTempAuctionCost = strTempAuctionCost.replace(",", "")

                                strTempAuctionCost = strTempAuctionCost.replace("(", "")
                                strTempAuctionCost = strTempAuctionCost.replace(")", "")
                                strTempAuctionCost = strTempAuctionCost.replace("%", "")

                                if len(strTempAuctionCost) > 0:
                                    arrActionCustInfo.append(strTempAuctionCost)

                        # print(arrActionCustInfo)
                        nAppraisalPrice = arrActionCustInfo[0]
                        nLowerPrice = arrActionCustInfo[1]
                        nRatio = str(0)
                        if len(arrActionCustInfo) > 2:
                            nRatio = arrActionCustInfo[2]

                        # print(arrActionCustInfo)
                        # ['277000000', '113459000', '40']
                        # 법원경매 5번째 컬럼 (감정평가액 / 최처매각가격) END


                        # 법원경매 6번째 컬럼 (담당계 / 매각기일) START
                        # print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), "================================================================================")
                        rstShowJpDeptInfoTitleInfos = rstTdElements[6]
                        rstShowJpDeptInfoTitles = repr(rstShowJpDeptInfoTitleInfos.text)
                        rstShowJpDeptInfoTitlesArrays = rstShowJpDeptInfoTitles.split('\\n')

                        arrShowJpDeptInfoTitle = []
                        for rstShowJpDeptInfoTitlesArray in rstShowJpDeptInfoTitlesArrays:
                            rstShowJpDeptInfoTitleText = GetLogDef.stripSpecharsForText(rstShowJpDeptInfoTitlesArray)
                            if len(rstShowJpDeptInfoTitleText) > 0:
                                # print(rstShowJpDeptInfoTitleText)
                                arrShowJpDeptInfoTitle.append(rstShowJpDeptInfoTitleText)

                        # aTextInfo = rstShowJpDeptInfoTitleInfos.select_one('a').get('onclick')
                        # print(aTextInfo)

                        # print(arrShowJpDeptInfoTitle) #for rstTrElement in rstTrElements: END

                        strAuctionPlace = arrShowJpDeptInfoTitle[0]
                        strAuctionDate = arrShowJpDeptInfoTitle[1].replace(".", "-") + " 00:00:00"
                        strBiddingInfo = arrShowJpDeptInfoTitle[2]


                        strUniqueValue = strAuctionUniqueNumber + "_" + strAuctionSeq + "_" + strCourtName + strAuctionDate
                        strUniqueValue2 = strAuctionUniqueNumber + "_" + strAuctionSeq + "_" + strCourtName + strAuctionDate+ "_" + strAuctionType
                        aes = AesCrypto.AESCipher('aesKey')
                        strUniqueValueEnc = aes.encrypt(strUniqueValue)
                        strUniqueValue2Enc = aes.encrypt(strUniqueValue2)


                        # ['경매21계', '2023.02.21', '유찰3회']
                        # 법원경매 6번째 컬럼 (감정평가액 / 최처매각가격) END
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "################################################################")
                        sqlCourtAuctionSelect = "SELECT * FROM " +ConstRealEstateTable_AUC.CourtAuctionDataTable +" WHERE unique_value2 = %s LIMIT 1 "
                        cursorRealEstate.execute(sqlCourtAuctionSelect, (strUniqueValue2Enc))
                        nResultCount = cursorRealEstate.rowcount
                        if nResultCount > 0:
                            continue


                        strDBState = '00'
                        sqlCourtAuctionSelect = "SELECT * FROM " +ConstRealEstateTable_AUC.CourtAuctionBackupTable +" WHERE unique_value2 = %s LIMIT 1 "
                        cursorRealEstate.execute(sqlCourtAuctionSelect, (strUniqueValue2Enc))
                        nResultCount = cursorRealEstate.rowcount
                        if nResultCount > 0:
                            strDBState = '10'

                        strJiBunAddress = ''
                        strLongitude = '000.00000000'  # 127
                        strLatitude = '000.00000000'  # 37
                        nProcessStep = str(00).zfill(2)
                        strBackAddressKeyword=''
                        strRoadName = ''

                        dtRegNow = DateTime.today()
                        dtResourceTomorrow = dtRegNow + TimeDelta(days=1)
                        dtBackupRegDate = str(dtRegNow.strftime('%Y-%m-%d 00:00:00'))
                        strTomorrowYMD = str(dtResourceTomorrow.strftime('%Y-%m-%d 00:00:00'))

                        #주소가 없는 경우는 Backup  테이블 에서 조회 Insert
                        # if len(jSonAddressInfo) <= 2:
                        rstBackupLists = cursorRealEstate.fetchall()
                        for rstBackupList in rstBackupLists:
                            jsonBackAddressData = rstBackupList.get('address_data')
                            strBackAddressKeyword = str(rstBackupList.get('address_keyword'))
                            strLongitude = str(rstBackupList.get('longitude'))
                            strLatitude = str(rstBackupList.get('latitude'))
                            strRoadName = GetLogDef.stripSpecharsForText(str(rstBackupList.get('road_name')))

                            strTextAddress = str(rstBackupList.get('text_address'))


                            strJiBunAddress = GetLogDef.stripSpecharsForText(strTextAddress)
                            nProcessStep = str(rstBackupList.get('process_step'))
                            dtBackupRegDate = str(rstBackupList.get('reg_date'))

                            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +  str(jsonBackAddressData))
                            if len(jsonBackAddressData) > 5:
                                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + str(type(strAuctionUniqueNumber)), str(len(strAuctionUniqueNumber)) , str(strAuctionUniqueNumber))
                                jSonAddressInfo = jsonBackAddressData
                                break


                        # # 도로명 주소 없으면
                        # if len(strRoadName) < 2:
                        #

                        #
                        #
                        #     strRoadName = GetRoadNameJuso.GetJusoApiForAddress(strJiBunAddress)

                        strIssueNumber = AuctionDataDecode.DecodeIssueNumber(jsonIssueNumber)

                        dictConversionAddress = GetRoadNameJuso.GetDictConversionAddress(logging, strIssueNumber,jSonAddressInfo)

                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dictConversionAddress] => " + str(dictConversionAddress))

                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dictConversionAddress] => " + str(dictConversionAddress['admCd']))

                        admCd = dictConversionAddress['admCd']

                        strDongmyunCode = admCd[5:10]

                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + " strDongmyunCode => " + str(strDongmyunCode))


                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+
                                     "[strAuctionUniqueNumber: ("+str(len(strAuctionUniqueNumber))+")" + str(strAuctionUniqueNumber))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strAuctionSeq: (" + str(len(strAuctionSeq)) + ")" + str( strAuctionSeq))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strCourtName: (" + str(len(strCourtName)) + ")" + str(strCourtName))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[jsonIssueNumber: (" + str(len(jsonIssueNumber)) + ")" + str(jsonIssueNumber))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[jsonUsageInfo: (" + str(len(jsonUsageInfo)) + ")" + str( jsonUsageInfo))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[jSonAddressInfo: (" + str(len(jSonAddressInfo)) + ")" + str(jSonAddressInfo))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strTempContents: (" + str(len(strTempContents)) + ")" + str(strTempContents))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[nAppraisalPrice: (" + str(len(nAppraisalPrice)) + ")" + str(nAppraisalPrice))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[nLowerPrice: (" + str(len(nLowerPrice)) + ")" + str(nLowerPrice))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[nRatio: (" + str(len(nRatio)) + ")" + str(nRatio))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strAuctionPlace: (" + str(len(strAuctionPlace)) + ")" + str(strAuctionPlace))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strAuctionDate: (" + str(len(strAuctionDate)) + ")" + str(strAuctionDate))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strAuctionType: (" + str(len(strAuctionType)) + ")" + str(strAuctionType))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strDBState: (" + str(len(strDBState)) + ")" + str(strDBState))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strJiBunAddress: (" + str(len(strJiBunAddress)) + ")" + str(strJiBunAddress))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strLongitude: (" + str(len(strLongitude)) + ")" + str(strLongitude))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strLatitude: (" + str(len(strLatitude)) + ")" + str(strLatitude))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[nProcessStep: (" + str(len(nProcessStep)) + ")" + str(nProcessStep))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strBiddingInfo: (" + str(len(strBiddingInfo)) + ")" + str(strBiddingInfo))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[CityKey: (" + str(len(CityKey)) + ")" + str(CityKey))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strSiGuCode: (" + str(len(strSiGuCode)) + ")" + str(strSiGuCode))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[dtBackupRegDate: (" + str(len(dtBackupRegDate)) + ")" + str(dtBackupRegDate))
                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[strDongmyunCode: ("+str(len(strDongmyunCode))+")" + str(strDongmyunCode))



                        sqlCourtAuctionInsert = " INSERT INTO " +ConstRealEstateTable_AUC.CourtAuctionDataTable +" SET "
                        sqlCourtAuctionInsert += " unique_value= '" + strUniqueValueEnc + "', "
                        sqlCourtAuctionInsert += " unique_value2= '" + strUniqueValue2Enc + "', "
                        sqlCourtAuctionInsert += " auction_code= '" + strAuctionUniqueNumber + "', "
                        sqlCourtAuctionInsert += " auction_seq= '" + strAuctionSeq + "', "
                        sqlCourtAuctionInsert += " court_name= '" + strCourtName + "', "
                        sqlCourtAuctionInsert += " issue_number= '" + jsonIssueNumber + "', "
                        sqlCourtAuctionInsert += " issue_number_text= '" + jsonIssueNumber + "', "
                        sqlCourtAuctionInsert += " build_type= '" + jsonUsageInfo + "', "
                        sqlCourtAuctionInsert += " build_type_text= '" + jsonUsageInfo + "', "
                        sqlCourtAuctionInsert += " build_type_code= '" + strBuildTypeCode + "', "
                        sqlCourtAuctionInsert += " sido_code= '" + CityKey + "', "
                        sqlCourtAuctionInsert += " sigu_code= '" + strSiGuCode + "', "
                        sqlCourtAuctionInsert += " dongmyun_code= '" + strDongmyunCode + "', "
                        sqlCourtAuctionInsert += " address_data= '" + jSonAddressInfo + "', "
                        sqlCourtAuctionInsert += " address_data_text= '" + jSonAddressInfo + "', "
                        sqlCourtAuctionInsert += " simple_info= '" + strTempContents + "', "
                        sqlCourtAuctionInsert += " appraisal_price= '" + nAppraisalPrice + "', "
                        sqlCourtAuctionInsert += " lower_price= '" + nLowerPrice + "', "
                        sqlCourtAuctionInsert += " ratio= '" + nRatio + "', "
                        sqlCourtAuctionInsert += " auction_place= '" + strAuctionPlace + "', "
                        sqlCourtAuctionInsert += " auction_day= '" + strAuctionDate + "', "
                        sqlCourtAuctionInsert += " auction_type= '" + strAuctionType + "', "
                        sqlCourtAuctionInsert += " text_address= '" + strJiBunAddress + "', "
                        sqlCourtAuctionInsert += " longitude= '" + strLongitude + "', "
                        sqlCourtAuctionInsert += " latitude= '" + strLatitude + "', "
                        sqlCourtAuctionInsert += " geo_point = ST_GeomFromText('POINT(" + strLongitude + " " + strLatitude + ")'), "
                        sqlCourtAuctionInsert += " address_keyword= '" + strBackAddressKeyword + "', "
                        sqlCourtAuctionInsert += " road_name= '" + strRoadName + "', "
                        sqlCourtAuctionInsert += " process_step= '" + nProcessStep + "', "
                        sqlCourtAuctionInsert += " state= '" + strDBState + "', "
                        sqlCourtAuctionInsert += " bidding_info= '" + strBiddingInfo + "', "
                        sqlCourtAuctionInsert += " reg_date= '" + dtBackupRegDate + "' "

                        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                                     "[sqlCourtAuctionInsert: (" + str(len(sqlCourtAuctionInsert)) + ")" + str(sqlCourtAuctionInsert))


                        cursorRealEstate.execute(sqlCourtAuctionInsert)

                        ResRealEstateConnection.commit()


                    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                    dictSwitchData = dict()
                    dictSwitchData['result'] = '10'
                    dictSwitchData['data_1'] = KuIndex
                    dictSwitchData['data_2'] = nAddressSiguSequence
                    dictSwitchData['data_3'] = targetRow
                    dictSwitchData['data_4'] = CityKey
                    dictSwitchData['data_5'] = strAuctionSeq
                    dictSwitchData['data_6'] = jsonIssueNumber
                    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

                    # 테스트 딜레이 추가
                    nRandomSec = random.randint(2,3)
                    # print(GetLogDef.lineno(), "Sleep! " + str(nRandomSec) + " Sec!")
                    time.sleep(nRandomSec)

                    targetRow = targetRow + paging #END While
                    logging.info("While END")

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                             "[=============END FOR for CityKey in AuctionCourtInfo.arrCityPlace ")
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                         "[=============for KuIndex in AuctionCourtInfo.dictAuctionTypes.items(): ")


        # logging.info("for KuIndex in AuctionCourtInfo.dictAuctionTypes.items():=====")

            # END While

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = KuIndex
        dictSwitchData['data_2'] = nAddressSiguSequence
        dictSwitchData['data_3'] = targetRow
        dictSwitchData['data_4'] = CityKey
        dictSwitchData['data_5'] = strAuctionSeq
        dictSwitchData['data_6'] = jsonIssueNumber
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if KuIndex is not None:
            dictSwitchData['data_1'] = KuIndex

        if CityKey is not None:
            dictSwitchData['data_2'] = nAddressSiguSequence

        if targetRow is not None:
            dictSwitchData['data_3'] = targetRow

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +"[=============[Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : ("+str(e)+")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                     "[=============[SUCCESS END]")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                     "[=============[CRONTAB END]")


if __name__ == '__main__':
    main()
