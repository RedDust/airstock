import requests


# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import datetime
sys.path.append("D:/PythonProjects/airstock")

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Const import ConstRealEstateTable_AUC
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Realty.Auction.Const import AuctionCourtInfo
from Init.DefConstant import ConstRealEstateTable
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable

try:
    print(GetLogDef.lineno(__file__), "============================================================")



    #https://curlconverter.com/ <- 프로그램 컨버터

    # 물건상세 검색
    # strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf"

    # 매각예정물건
    #strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveMgakPlanMulSrch.laf"


    # 매각결과
    #strCourtAuctionUrl = "https://www.courtauction.go.kr/RetrieveRealEstMgakGyulgwaMulList.laf"

    strProcessType = '020000'
    KuIndex = '00'
    arrCityPlace = '00'
    targetRow = '00'

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
    dictSwitchData['data_2'] = arrCityPlace
    dictSwitchData['data_3'] = targetRow
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

    print(GetLogDef.lineno(__file__), "==================================================================")
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
        for arrCityPlace in AuctionCourtInfo.arrCityPlace:

            print(GetLogDef.lineno(__file__), "==================================================================")
            print(arrCityPlace)

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
                    'daepyoSidoCd': arrCityPlace,
                    'mvmPlaceSidoCd': '',
                    'JSESSIONID': 'GaL7Ehe4mYWbEDHCagGn6L60inn16EKTWsNnMnXVnjQRRAlVbAGe1zK51jnl3BZW.amV1c19kb21haW4vYWlzMg==',
                }


                data = 'page=default'+str(paging)+'&' \
                       'srnID=PNO102000&jiwonNm=&bubwLocGubun=2&jibhgwanOffMgakPlcGubun=&mvmPlaceSidoCd=&mvmPlaceSiguCd=&roadPlaceSidoCd=&' \
                       'roadPlaceSiguCd=&daepyoSidoCd='+arrCityPlace+'&daepyoSiguCd=&daepyoDongCd=&rd1Cd=&rd2Cd=&rd3Rd4Cd=&roadCode=&' \
                       'notifyLoc=1&notifyRealRoad=1&notifyNewLoc=1&mvRealGbncd=1&jiwonNm1=%C0%FC%C3%BC&jiwonNm2=%BC%AD%BF%EF%C1%DF%BE%D3%C1%F6%B9%E6%B9%FD%BF%F8&' \
                       'mDaepyoSidoCd='+arrCityPlace+'&mvDaepyoSidoCd=&mDaepyoSiguCd=&mvDaepyoSiguCd=&realVowel=00000_55203&vowelSel=00000_55203&mDaepyoDongCd=&mvmPlaceDongCd=&' \
                       '_NAVI_CMD=&_NAVI_SRNID=&_SRCH_SRNID=PNO102000&_CUR_CMD=RetrieveMainInfo.laf&_CUR_SRNID=PNO102000&_NEXT_CMD=&_NEXT_SRNID=PNO102002&' \
                       '_PRE_SRNID=PNO102001&_LOGOUT_CHK=&_FORM_YN=Y&PNIPassMsg=%C1%A4%C3%A5%BF%A1+%C0%C7%C7%D8+%C2%F7%B4%DC%B5%C8+%C7%D8%BF%DCIP+%BB%E7%BF%EB%C0%DA%C0%D4%B4%CF%B4%D9.&' \
                       'pageSpec=default'+str(paging)+'&pageSpec=default'+str(paging)+'&' \
                       'targetRow='+str(targetRow)+'&lafjOrderBy='


                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = KuIndex
                dictSwitchData['data_2'] = arrCityPlace
                dictSwitchData['data_3'] = targetRow
                LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

                response = requests.post(
                    strCourtAuctionUrl,
                    cookies=cookies,
                    headers=headers,
                    data=data,
                )

                html = response.text  # page_source 얻기
                soup = BeautifulSoup(html, "html.parser")  # get html
                rstMainElements = soup.select_one('#contents > div.table_contents > form:nth-child(1) > table > tbody')

                nLoopTrElements = 0
                rstTrElements = rstMainElements.select('tr')
                print(GetLogDef.lineno(__file__), "-------------------------------------------------------------------------------")
                strErrorMessage = rstMainElements.select_one('tr').select_one('td').text

                if strErrorMessage == '검색결과가 없습니다.':
                    print(GetLogDef.lineno(__file__), "---------------------------------------------["+str(targetRow)+"]수집완료")
                    break

                for rstTrElement in rstTrElements:
                    print(GetLogDef.lineno(__file__), "[type:"+str(strAuctionType)+"][place:"+str(arrCityPlace)+"][page:"+str(targetRow)+"][count:"+str(nLoopTrElements)+"]+++++++++")
                    nLoopTrElements = nLoopTrElements + 1

                    nLoopTdElements = 0
                    rstTdElements = rstTrElement.select('td')


                    # DB 연결
                    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
                    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


                    # 법원경매 0번째 컬럼 (CHECKBOX - 법정,고유코드,) START
                    strCheckBoxValues = (rstTdElements[0].select_one('input[type=checkbox]').get('value'))
                    arrCheckBoxValues = strCheckBoxValues.split(',')
                    if arrCheckBoxValues != 3:
                        Exception(GetLogDef.lineno(__file__), 'arrCheckBoxValues => ', arrCheckBoxValues)  # 예외를 발생시킴

                    # print(arrCheckBoxValues)
                    strCourtName = arrCheckBoxValues[0]
                    strAuctionUniqueNumber = arrCheckBoxValues[1]
                    strAuctionSeq = arrCheckBoxValues[2]
                    # 법원경매 0번째 컬럼 (CHECKBOX) END

                    # 법원경매 1번째 컬럼 (사건번호) START
                    print(GetLogDef.lineno(__file__), "-------------------------------------------------------------------------------")
                    rstIssueNumber = rstTdElements[1]
                    rstIssueNumber.find('input').decompose()
                    strIssueNumber = rstIssueNumber.text
                    print(rstIssueNumber,type(rstIssueNumber))

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
                        Exception(GetLogDef.lineno(__file__), 'arrTempIssueNumber[0] => ', arrTempIssueNumber[0])  # 예외를 발생시킴

                    # 사건 번호 정보가 없으면 오류
                    if len(arrTempIssueNumber) < 2:
                        Exception(GetLogDef.lineno(__file__), 'len(arrTempIssueNumber) => ', len(arrTempIssueNumber))  # 예외를 발생시킴

                    # #중복 물건 또는 병합물건 존재함
                    # if len(arrTempIssueNumber) > 2:
                    #     print("어레이가 2개 이상", len(arrTempIssueNumber))

                    jsonIssueNumber = json.dumps(arrTempIssueNumber, ensure_ascii=False)
                    # print(jsonIssueNumber)
                    # jsonIssueNumber
                    # 법원경매 1번째 컬럼 (사건번호) END

                    # 법원경매 2번째 컬럼 (용도번호) START
                    print(GetLogDef.lineno(__file__), "================================================================================")
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
                        Exception(GetLogDef.lineno(__file__), 'len(arrTempIssueNumber) => ', len(arrTempUsageInfo))  # 예외를 발생시킴

                    # #중복 물건 또는 병합물건 존재함
                    # if len(arrTempUsageInfo) > 2:
                    #     print("어레이가 2개 이상", len(arrTempUsageInfo))

                    jsonUsageInfo = json.dumps(arrTempUsageInfo, ensure_ascii=False)
                    # print(jsonUsageInfo)

                    # ["'1'", "'대지'", "'임야'", "'전답'"]
                    # 법원경매 2번째 컬럼 (용도번호) END

                    # 법원경매 3번째 컬럼 (소재지 및 내역) START
                    print(GetLogDef.lineno(__file__), "================================================================================")
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
                    print(GetLogDef.lineno(__file__), "================================================================================")
                    rstAuctionCosts = rstTdElements[5]
                    rstDivs = rstAuctionCosts.select('div')
                    nLoopTempAddressInfo = 0
                    arrActionCustInfo = []
                    for rstDiv in rstDivs:
                        print(GetLogDef.lineno(__file__),"################################################################")
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
                    print(GetLogDef.lineno(__file__), "################################################################")
                    if len(arrActionCustInfo) > 2:
                        nRatio = arrActionCustInfo[2]

                    print(GetLogDef.lineno(__file__), "################################################################")
                    # print(arrActionCustInfo)
                    # ['277000000', '113459000', '40']
                    # 법원경매 5번째 컬럼 (감정평가액 / 최처매각가격) END


                    # 법원경매 6번째 컬럼 (담당계 / 매각기일) START
                    # print(GetLogDef.lineno(__file__), "================================================================================")
                    rstShowJpDeptInfoTitleInfos = rstTdElements[6]
                    rstShowJpDeptInfoTitles = repr(rstShowJpDeptInfoTitleInfos.text)
                    rstShowJpDeptInfoTitlesArrays = rstShowJpDeptInfoTitles.split('\\n')
                    print(GetLogDef.lineno(__file__), "################################################################")
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
                    # ['경매21계', '2023.02.21', '유찰3회']
                    # 법원경매 6번째 컬럼 (감정평가액 / 최처매각가격) END
                    print(GetLogDef.lineno(__file__), "################################################################")
                    sqlCourtAuctionSelect = "SELECT * FROM " +ConstRealEstateTable_AUC.CourtAuctionDataTable +" WHERE auction_code = %s AND auction_seq = %s LIMIT 1 "
                    cursorRealEstate.execute(sqlCourtAuctionSelect, (strAuctionUniqueNumber, strAuctionSeq))
                    nResultCount = cursorRealEstate.rowcount
                    if nResultCount > 0:
                        continue


                    strDBState = '00'
                    sqlCourtAuctionSelect = "SELECT * FROM " +ConstRealEstateTable_AUC.CourtAuctionBackupTable +" WHERE auction_code = %s AND auction_seq = %s LIMIT 1 "
                    cursorRealEstate.execute(sqlCourtAuctionSelect, (strAuctionUniqueNumber, strAuctionSeq))
                    nResultCount = cursorRealEstate.rowcount
                    if nResultCount > 0:
                        strDBState = '10'

                    # nAuctionCode = str(SelectColumnList.get('auction_code'))
                    # nAuctionSeq = str(SelectColumnList.get('auction_seq'))
                    # strCourtName = str(SelectColumnList.get('court_name'))
                    # dtAuctionDay = str(SelectColumnList.get('auction_day'))

                    strUniqueValue = strAuctionUniqueNumber + "_" + strAuctionSeq + "_" + strCourtName + strAuctionDate


                    print(GetLogDef.lineno(__file__), "strAuctionUniqueNumber => ", strAuctionUniqueNumber, type(strAuctionUniqueNumber))
                    print(GetLogDef.lineno(__file__), "strAuctionSeq => ", strAuctionSeq, type(strAuctionSeq))
                    print(GetLogDef.lineno(__file__), "strCourtName => ", strCourtName, type(strCourtName))
                    print(GetLogDef.lineno(__file__), "jsonIssueNumber => ", jsonIssueNumber, type(jsonIssueNumber))
                    print(GetLogDef.lineno(__file__), "jsonUsageInfo => ", jsonUsageInfo, type(jsonUsageInfo))
                    print(GetLogDef.lineno(__file__), "jSonAddressInfo => ", jSonAddressInfo, type(jSonAddressInfo))
                    print(GetLogDef.lineno(__file__), "strTempContents => ", strTempContents, type(strTempContents))
                    print(GetLogDef.lineno(__file__), "nAppraisalPrice => ", nAppraisalPrice, type(nAppraisalPrice))
                    print(GetLogDef.lineno(__file__), "nLowerPrice => ", nLowerPrice, type(nLowerPrice))
                    print(GetLogDef.lineno(__file__), "nRatio => ", nRatio, type(nRatio))
                    print(GetLogDef.lineno(__file__), "strAuctionPlace => ", strAuctionPlace, type(strAuctionPlace))
                    print(GetLogDef.lineno(__file__), "strAuctionDate => ", strAuctionDate, type(strAuctionDate))
                    print(GetLogDef.lineno(__file__), "strAuctionType => ", strAuctionType, type(strAuctionType))
                    print(GetLogDef.lineno(__file__), "strDBState => ", strDBState, type(strDBState))
                    print(GetLogDef.lineno(__file__), "strBiddingInfo => ", strBiddingInfo, type(strBiddingInfo))


                    sqlCourtAuctionInsert = " INSERT INTO " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " SET "
                    sqlCourtAuctionInsert += " unique_value= '" + strUniqueValue + "', "
                    sqlCourtAuctionInsert += " auction_code= '" + strAuctionUniqueNumber + "', "
                    sqlCourtAuctionInsert += " auction_seq= '" + strAuctionSeq + "', "
                    sqlCourtAuctionInsert += " court_name= '" + strCourtName + "', "
                    sqlCourtAuctionInsert += " issue_number= '" + jsonIssueNumber + "', "
                    sqlCourtAuctionInsert += " issue_number_text= '" + jsonIssueNumber + "', "
                    sqlCourtAuctionInsert += " build_type= '" + jsonUsageInfo + "', "
                    sqlCourtAuctionInsert += " build_type_text= '" + jsonUsageInfo + "', "
                    sqlCourtAuctionInsert += " address_data= '" + jSonAddressInfo + "', "
                    sqlCourtAuctionInsert += " address_data_text= '" + jSonAddressInfo + "', "
                    sqlCourtAuctionInsert += " simple_info= '" + strTempContents + "', "
                    sqlCourtAuctionInsert += " appraisal_price= '" + nAppraisalPrice + "', "
                    sqlCourtAuctionInsert += " lower_price= '" + nLowerPrice + "', "
                    sqlCourtAuctionInsert += " ratio= '" + nRatio + "', "
                    sqlCourtAuctionInsert += " auction_place= '" + strAuctionPlace + "', "
                    sqlCourtAuctionInsert += " auction_day= '" + strAuctionDate + "', "
                    sqlCourtAuctionInsert += " auction_type= '" + strAuctionType + "', "
                    sqlCourtAuctionInsert += " state= '" + strDBState + "', "
                    sqlCourtAuctionInsert += " bidding_info= '" + strBiddingInfo + "' "

                    print("sqlCourtAuctionInsert > ", sqlCourtAuctionInsert)
                    cursorRealEstate.execute(sqlCourtAuctionInsert)

                    ResRealEstateConnection.commit()

                # 테스트 딜레이 추가
                nRandomSec = random.randint(1, 2)
                # print(GetLogDef.lineno(), "Sleep! " + str(nRandomSec) + " Sec!")
                time.sleep(nRandomSec)

                targetRow = targetRow + paging #END While

            print("[" + arrCityPlace + "]END FOR for arrCityPlace in AuctionCourtInfo.arrCityPlace ")
        print("["+KuIndex+"]for KuIndex in AuctionCourtInfo.dictAuctionTypes.items():")

    # print("for KuIndex in AuctionCourtInfo.dictAuctionTypes.items():=====")

        # END While

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_1'] = KuIndex
    dictSwitchData['data_2'] = arrCityPlace
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

    if arrCityPlace is not None:
        dictSwitchData['data_2'] = arrCityPlace

    if targetRow is not None:
        dictSwitchData['data_3'] = targetRow

    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    print(GetLogDef.lineno(__file__), "Error Exception")
    print(GetLogDef.lineno(__file__), e, type(e))

else:
    print(GetLogDef.lineno(__file__), "============================================================")

finally:
    print("Finally END")


