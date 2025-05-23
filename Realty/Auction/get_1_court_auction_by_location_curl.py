quit(500)
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


def main():

    try:
        print(GetLogDef.lineno(__file__), "============================================================")

        #https://curlconverter.com/ <- 프로그램 컨버터

        # 물건상세 검색
        # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf"

        # 매각예정물건
        #strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveMgakPlanMulSrch.laf"


        # 매각결과
        #strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMgakGyulgwaMulList.laf"

        strProcessType = ''
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




        # # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        # rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        # strResult = rstResult.get('result')
        # if strResult is False:
        #     quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴
        #
        # if strResult == '10':
        #     quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴
        #
        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '10'
        # dictSwitchData['data_1'] = KuIndex
        # dictSwitchData['data_2'] = CityKey
        # dictSwitchData['data_3'] = targetRow
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)
        #
        # logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
        #                                inspect.getframeinfo(inspect.currentframe()).lineno) +
        #              "[CRONTAB START]==================================================================")


        # 기초 헤더 정리
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://www.courtauction.go.kr',
            'Referer': 'https://www.courtauction.go.kr/RetrieveMainInfo.laf',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            # 'Cookie': 'WMONID=sfXO0bVObsW; mvmPlaceSidoCd=; mvmPlaceSiguCd=; rd1Cd=; rd2Cd=; roadPlaceSidoCd=; roadPlaceSiguCd=; vowelSel=35207_45207; page=default40; toMul=^%^C0^%^C7^%^C1^%^A4^%^BA^%^CE^%^C1^%^F6^%^B9^%^E6^%^B9^%^FD^%^BF^%^F8^%^2C20220130003915^%^2C1^%^2C20231108^%^2CB^%^5E^%^BC^%^AD^%^BF^%^EF^%^C1^%^DF^%^BE^%^D3^%^C1^%^F6^%^B9^%^E6^%^B9^%^FD^%^BF^%^F8^%^2C20220130112701^%^2C1^%^2C20231031^%^2CB^%^5E^%^BC^%^AD^%^BF^%^EF^%^C1^%^DF^%^BE^%^D3^%^C1^%^F6^%^B9^%^E6^%^B9^%^FD^%^BF^%^F8^%^2C20200130003605^%^2C3^%^2C20231107^%^2CB^%^5E^%^BA^%^CE^%^BB^%^EA^%^C1^%^F6^%^B9^%^E6^%^B9^%^FD^%^BF^%^F8^%^2C20120130023039^%^2C1^%^2C20231031^%^2CB^%^5E; realJiwonNm=^%^BC^%^AD^%^BF^%^EF^%^C1^%^DF^%^BE^%^D3^%^C1^%^F6^%^B9^%^E6^%^B9^%^FD^%^BF^%^F8; daepyoSidoCd=43; daepyoSiguCd=750; realVowel=35207_45207; JSESSIONID=a819VY1QjBFYPFtTHtDSuj2Avomq01OBuoR6J35bKqAKH2S0n0SgCEoPg1AaYO2B.amV1c19kb21haW4vYWlzMg==',
            'Origin': 'https://www.courtauction.go.kr',
            'Referer': 'https://www.courtauction.go.kr/RetrieveMainInfo.laf',
            'Sec-Fetch-Dest': 'frame',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'sec-ch-ua': '^\\^Chromium^^;v=^\\^118^^, ^\\^Google',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '^\\^Windows^^',
        }



        # 초기 값
        paging = 40

        # quit(GetLogDef.lineno(__file__))  # 예외를 발생시킴
        #
        for KuIndex, AuctionCallInfo in AuctionCourtInfo.dictAuctionTypes.items():

            print(KuIndex)
            print(AuctionCallInfo)
            print(GetLogDef.lineno(__file__), "==================================================================")
            # print(AuctionCourtInfo.dictAuctionTypes['1'].get('url'))
            # print(AuctionCourtInfo.dictAuctionTypes['1'].get('type'))

            strCourtAuctionUrl = AuctionCourtInfo.dictAuctionTypes[KuIndex].get('url')
            strAuctionType = AuctionCourtInfo.dictAuctionTypes[KuIndex].get('type')

            # print(strCourtAuctionUrl)
            # print(strCourtAuctionUrl)
            # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

            qrySelectRoad  = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionAddressSiguTable
            # qrySelectRoad = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionAddressRoadTable
            # qrySelectRoad += " WHERE sido_code='11' "
            # AND sigu_code='740' AND road_code='4172162' "
            # qrySelectRoad += " LIMIT 1"

            cursorRealEstate.execute(qrySelectRoad)
            rstSiDoLists = cursorRealEstate.fetchall()

            for rstSiDoList in rstSiDoLists:

            # for CityKey, CityValue in AuctionCourtInfo.dictCityPlace.items():

                CityKey = str(rstSiDoList.get('sido_code'))
                strSiGuCode = str(rstSiDoList.get('sigu_code'))

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


                    data = 'page=default40&srnID=PNO102000&jiwonNm=&bubwLocGubun=2&jibhgwanOffMgakPlcGubun=' \
                           '&mvmPlaceSidoCd=&mvmPlaceSiguCd=&roadPlaceSidoCd=&roadPlaceSiguCd=' \
                           '&daepyoSidoCd='+CityKey+'&daepyoSiguCd='+strSiGuCode+'&daepyoDongCd=103&rd1Cd=&rd2Cd=&rd3Rd4Cd=&roadCode=' \
                           '&notifyLoc=1&notifyRealRoad=1&notifyNewLoc=1&mvRealGbncd=1' \
                           '&jiwonNm1=^%^C0^%^FC^%^C3^%^BC&jiwonNm2=^%^BC^%^AD^%^BF^%^EF^%^C1^%^DF^%^BE^%^D3^%^C1^%^F6^%^B9^%^E6^%^B9^%^FD^%^BF^%^F8' \
                           '&mDaepyoSidoCd='+CityKey+'&mvDaepyoSidoCd=&mDaepyoSiguCd='+strSiGuCode+'' \
                           '&mvDaepyoSiguCd=&realVowel=00000_55203&vowelSel=00000_55203&mDaepyoDongCd=103^%^7CA' \
                           '&mvmPlaceDongCd=&_NAVI_CMD=&_NAVI_SRNID=&_SRCH_SRNID=PNO102000&_CUR_CMD=RetrieveMainInfo.laf&_CUR_SRNID=PNO102000&_NEXT_CMD=' \
                           '&_NEXT_SRNID=PNO102002&_PRE_SRNID=PNO102001&_LOGOUT_CHK=&_FORM_YN=Y&page=default40' \
                           '&PNIPassMsg=^%^C1^%^A4^%^C3^%^A5^%^BF^%^A1+^%^C0^%^C7^%^C7^%^D8+^%^C2^%^F7^%^B4^%^DC^%^B5^%^C8+^%^C7^%^D8^%^BF^%^DCIP+^%^BB^%^E7^%^BF^%^EB^%^C0^%^DA^%^C0^%^D4^%^B4^%^CF^%^B4^%^D9.' \
                           '&pageSpec=default40&targetRow='+str(targetRow)+'&lafjOrderBy=order+by+maeGiil+asc'


                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "data = ["+ str(data) + "] 수집")


                    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                    dictSwitchData = dict()
                    dictSwitchData['result'] = '10'
                    dictSwitchData['data_1'] = KuIndex
                    dictSwitchData['data_2'] = CityKey
                    dictSwitchData['data_3'] = targetRow
                    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

                    response = requests.post(
                        strCourtAuctionUrl,
                        cookies=cookies,
                        headers=headers,
                        data=data,
                    )

                    html = response.text  # page_source 얻기

                    # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                                inspect.getframeinfo(
                    #                                    inspect.currentframe()).lineno) + "html = [" + str(
                    #     html) + "]")

                    soup = BeautifulSoup(html, "html.parser")  # get html
                    rstMainElements = soup.select_one('#contents > div.table_contents > form:nth-child(1) > table > tbody')
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "rstMainElements = [" + str(
                        rstMainElements) + "]")

                    # if str(rstMainElements) == 'None':
                    #     logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                                    inspect.getframeinfo(inspect.currentframe()).lineno) +
                    #                  str(targetRow) + "]None")
                    #     break

                    # logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename, inspect.getframeinfo(inspect.currentframe()).lineno) + "rstMainElements = ["+ str(rstMainElements) + "]")


                    nLoopTrElements = 0
                    rstTrElements = rstMainElements.select('tr')
                    # print(GetLogDef.lineno(__file__), "-------------------------------------------------------------------------------")
                    strErrorMessage = rstMainElements.select_one('tr').select_one('td').text

                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(
                                                       inspect.currentframe()).lineno) + "strErrorMessage = [" + str(
                        strErrorMessage) + "] strErrorMessage ")

                    if strErrorMessage == '검색결과가 없습니다.':
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     str(targetRow) + "]수집완료")
                        break

                    for rstTrElement in rstTrElements:
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[type : " + str(strAuctionType) + "][place:" + str(CityKey) +
                                     "][page : " + str(targetRow) + "][count :" + str(nLoopTrElements) +
                                     "]+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                        nLoopTrElements = nLoopTrElements + 1

                        nLoopTdElements = 0
                        rstTdElements = rstTrElement.select('td')




                        # 법원경매 0번째 컬럼 (CHECKBOX - 법정,고유코드,) START
                        strCheckBoxValues = (rstTdElements[0].select_one('input[type=checkbox]').get('value'))
                        arrCheckBoxValues = strCheckBoxValues.split(',')
                        if len(arrCheckBoxValues) != 3:
                            raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                              inspect.getframeinfo(inspect.currentframe()).lineno),
                                            'arrCheckBoxValues => ', arrCheckBoxValues)  # 예외를 발생시킴

                        # print(arrCheckBoxValues)
                        strCourtName = arrCheckBoxValues[0]
                        strAuctionUniqueNumber = arrCheckBoxValues[1]
                        strAuctionSeq = arrCheckBoxValues[2]
                        # 법원경매 0번째 컬럼 (CHECKBOX) END

                        # 법원경매 1번째 컬럼 (사건번호) START
                        rstIssueNumber = rstTdElements[1]
                        rstIssueNumber.find('input').decompose()
                        strIssueNumber = rstIssueNumber.text
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
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
                            raise Exception(GetLogDef.lineno(__file__), 'arrTempIssueNumber[0] => ', arrTempIssueNumber[0])  # 예외를 발생시킴

                        # 사건 번호 정보가 없으면 오류
                        if len(arrTempIssueNumber) < 2:
                            raise Exception(GetLogDef.lineno(__file__), 'len(arrTempIssueNumber) => ', len(arrTempIssueNumber))  # 예외를 발생시킴

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
                            raise Exception(GetLogDef.lineno(__file__), 'len(arrTempIssueNumber) => ', len(arrTempUsageInfo))  # 예외를 발생시킴

                        # #중복 물건 또는 병합물건 존재함
                        # if len(arrTempUsageInfo) > 2:
                        #     print("어레이가 2개 이상", len(arrTempUsageInfo))

                        jsonUsageInfo = json.dumps(arrTempUsageInfo, ensure_ascii=False)
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
                            # print(GetLogDef.lineno(__file__),"-------------------------------------------------------------------------------")
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
                        # print(GetLogDef.lineno(__file__), "================================================================================")
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
                        # print(GetLogDef.lineno(__file__), "################################################################")
                        sqlCourtAuctionSelect = "SELECT * FROM kt_realty_court_auction_master_temp7 WHERE unique_value = %s LIMIT 1 "
                        cursorRealEstate.execute(sqlCourtAuctionSelect, (strUniqueValueEnc))
                        nResultCount = cursorRealEstate.rowcount
                        if nResultCount > 0:
                            continue


                        strDBState = '00'
                        sqlCourtAuctionSelect = "SELECT * FROM " +ConstRealEstateTable_AUC.CourtAuctionBackupTable +" WHERE unique_value = %s LIMIT 1 "
                        cursorRealEstate.execute(sqlCourtAuctionSelect, (strUniqueValueEnc))
                        nResultCount = cursorRealEstate.rowcount
                        if nResultCount > 0:
                            strDBState = '10'

                        strJiBunAddress = ''
                        strLongitude = '000.00000000'  # 127
                        strLatitude = '000.00000000'  # 37
                        nProcessStep = str(00).zfill(2)
                        #주소가 없는 경우는 Backup  테이블 에서 조회 Insert
                        # if len(jSonAddressInfo) <= 2:
                        rstBackupLists = cursorRealEstate.fetchall()
                        for rstBackupList in rstBackupLists:
                            jsonBackAddressData = rstBackupList.get('address_data')
                            strLongitude = str(rstBackupList.get('longitude'))
                            strLatitude = str(rstBackupList.get('latitude'))
                            strJiBunAddress = str(rstBackupList.get('text_address'))
                            strJiBunAddress = GetLogDef.stripSpecharsForText(strJiBunAddress)
                            nProcessStep = str(rstBackupList.get('process_step'))

                            # print(GetLogDef.lineno(__file__), jsonBackAddressData ,'-------------------------------------------------------------')
                            if len(jsonBackAddressData) > 5:
                                print(GetLogDef.lineno(__file__), type(strAuctionUniqueNumber), len(strAuctionUniqueNumber), strAuctionUniqueNumber)
                                print(GetLogDef.lineno(__file__), ">>", type(jsonBackAddressData), len(jsonBackAddressData), jsonBackAddressData)
                                jSonAddressInfo = jsonBackAddressData
                                # break

                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strAuctionUniqueNumber: ("+str(len(strAuctionUniqueNumber))+")" + str(strAuctionUniqueNumber))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strAuctionSeq: (" + str(len(strAuctionSeq)) + ")" + str( strAuctionSeq))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strCourtName: (" + str(len(strCourtName)) + ")" + str(strCourtName))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[jsonIssueNumber: (" + str(len(jsonIssueNumber)) + ")" + str(jsonIssueNumber))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[jsonUsageInfo: (" + str(len(jsonUsageInfo)) + ")" + str( jsonUsageInfo))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[jSonAddressInfo: (" + str(len(jSonAddressInfo)) + ")" + str(jSonAddressInfo))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strTempContents: (" + str(len(strTempContents)) + ")" + str(strTempContents))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[nAppraisalPrice: (" + str(len(nAppraisalPrice)) + ")" + str(nAppraisalPrice))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[nLowerPrice: (" + str(len(nLowerPrice)) + ")" + str(nLowerPrice))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[nRatio: (" + str(len(nRatio)) + ")" + str(nRatio))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strAuctionPlace: (" + str(len(strAuctionPlace)) + ")" + str(strAuctionPlace))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strAuctionDate: (" + str(len(strAuctionDate)) + ")" + str(strAuctionDate))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strAuctionType: (" + str(len(strAuctionType)) + ")" + str(strAuctionType))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strDBState: (" + str(len(strDBState)) + ")" + str(strDBState))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strJiBunAddress: (" + str(len(strJiBunAddress)) + ")" + str(strJiBunAddress))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strLongitude: (" + str(len(strLongitude)) + ")" + str(strLongitude))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strLatitude: (" + str(len(strLatitude)) + ")" + str(strLatitude))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[nProcessStep: (" + str(len(nProcessStep)) + ")" + str(nProcessStep))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strBiddingInfo: (" + str(len(strBiddingInfo)) + ")" + str(strBiddingInfo))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[CityKey: (" + str(len(CityKey)) + ")" + str(CityKey))
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strSiGuCode: (" + str(len(strSiGuCode)) + ")" + str(strSiGuCode))
                        # logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                        #                                inspect.getframeinfo(inspect.currentframe()).lineno) +
                        #              "[strSiGuName: (" + str(len(strSiGuName)) + ")" + str(strSiGuName))

                        sqlCourtAuctionInsert = " INSERT INTO kt_realty_court_auction_master_temp7 SET "
                        sqlCourtAuctionInsert += " unique_value= '" + strUniqueValueEnc + "', "
                        sqlCourtAuctionInsert += " unique_value2= '" + strUniqueValue2Enc + "', "
                        sqlCourtAuctionInsert += " auction_code= '" + strAuctionUniqueNumber + "', "
                        sqlCourtAuctionInsert += " auction_seq= '" + strAuctionSeq + "', "
                        sqlCourtAuctionInsert += " court_name= '" + strCourtName + "', "
                        sqlCourtAuctionInsert += " issue_number= '" + jsonIssueNumber + "', "
                        sqlCourtAuctionInsert += " issue_number_text= '" + jsonIssueNumber + "', "
                        sqlCourtAuctionInsert += " build_type= '" + jsonUsageInfo + "', "
                        sqlCourtAuctionInsert += " build_type_text= '" + jsonUsageInfo + "', "
                        sqlCourtAuctionInsert += " sido_code= '" + CityKey + "', "
                        sqlCourtAuctionInsert += " sigu_code= '" + strSiGuCode + "', "
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
                        sqlCourtAuctionInsert += " process_step= '" + nProcessStep + "', "
                        sqlCourtAuctionInsert += " state= '" + strDBState + "', "
                        sqlCourtAuctionInsert += " bidding_info= '" + strBiddingInfo + "' "

                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[sqlCourtAuctionInsert: (" + str(len(sqlCourtAuctionInsert)) + ")" + str(sqlCourtAuctionInsert))


                        cursorRealEstate.execute(sqlCourtAuctionInsert)

                        ResRealEstateConnection.commit()

                    # 테스트 딜레이 추가
                    nRandomSec = random.randint(1, 2)
                    # print(GetLogDef.lineno(), "Sleep! " + str(nRandomSec) + " Sec!")
                    time.sleep(nRandomSec)

                    targetRow = targetRow + paging #END While
                    print("NEXT While END")

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                               inspect.getframeinfo(inspect.currentframe()).lineno) +
                             "[" + "]END FOR for CityKey in AuctionCourtInfo.arrCityPlace ")
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                           inspect.getframeinfo(inspect.currentframe()).lineno) +
                         "[" + KuIndex + "]for KuIndex in AuctionCourtInfo.dictAuctionTypes.items():")


        # print("for KuIndex in AuctionCourtInfo.dictAuctionTypes.items():=====")

            # END While

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


