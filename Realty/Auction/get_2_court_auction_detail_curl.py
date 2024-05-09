# 경매 사건 검색


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

from io import StringIO
from html.parser import HTMLParser

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue()

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

try:
    print(GetLogDef.lineno(__file__), "============================================================")

    #https://curlconverter.com/ <- 프로그램 컨버터

    #디테일 데이터 수집
    strProcessType = '020010'
    KuIndex = '00'
    arrCityPlace = '00'
    targetRow = '00'


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

    # if strResult == '10':
    #     quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '10'
    dictSwitchData['data_1'] = KuIndex
    dictSwitchData['data_2'] = arrCityPlace
    dictSwitchData['data_3'] = targetRow
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)



    cookies = {
        'WMONID': 'PIgokmmbg9M',
        'daepyoSiguCd': '',
        'mvmPlaceSidoCd': '',
        'mvmPlaceSiguCd': '',
        'rd1Cd': '',
        'rd2Cd': '',
        'realVowel': '35207_45207',
        'roadPlaceSidoCd': '',
        'roadPlaceSiguCd': '',
        'vowelSel': '35207_45207',
        'page': 'default40',
        'toMul': '%BC%AD%BF%EF%C1%DF%BE%D3%C1%F6%B9%E6%B9%FD%BF%F8%2C20190130104335%2C1%2C20230221%2CB%5E',
        'JSESSIONID': 'ToB7R4nR7qHf7eHQcylkhtFigBjayMcBkqSLjX1zbmkLqROEbL1iqPIDXpvthN8O.amV1c19kb21haW4vYWlzMg==',
        'locIdx': '',
        'daepyoSidoCd': '',
        'realJiwonNm': '%C0%CE%C3%B5%C1%F6%B9%E6%B9%FD%BF%F8',
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        # 'Cookie': 'WMONID=PIgokmmbg9M; daepyoSiguCd=; mvmPlaceSidoCd=; mvmPlaceSiguCd=; rd1Cd=; rd2Cd=; realVowel=35207_45207; roadPlaceSidoCd=; roadPlaceSiguCd=; vowelSel=35207_45207; page=default40; toMul=%BC%AD%BF%EF%C1%DF%BE%D3%C1%F6%B9%E6%B9%FD%BF%F8%2C20190130104335%2C1%2C20230221%2CB%5E; JSESSIONID=ToB7R4nR7qHf7eHQcylkhtFigBjayMcBkqSLjX1zbmkLqROEbL1iqPIDXpvthN8O.amV1c19kb21haW4vYWlzMg==; locIdx=; daepyoSidoCd=; realJiwonNm=%C0%CE%C3%B5%C1%F6%B9%E6%B9%FD%BF%F8',
        'Origin': 'https://www.courtauction.go.kr',
        'Referer': 'https://www.courtauction.go.kr/InitMulSrch.laf',
        'Sec-Fetch-Dest': 'frame',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }

#["여주지원", "2022타경30896"]

    strJiwonName = "서울중앙지방법원"
    strEncodedJiwonNm = AuctionCourtInfo.dictCourtInfo.get(strJiwonName)['encoded']

    saYear = str(2020)
    saSer = str(107200)


    strSaYear = str(2021)
    strSaSer = str(106815)


    data = 'srnID=PNO102014' \
           '&_CUR_CMD=InitMulSrch.laf' \
           '&jiwonNm='+strEncodedJiwonNm + \
           '&saYear='+strSaYear + \
           '&saSer='+strSaSer + \
           '&_NAVI_CMD=&_NAVI_SRNID=&_SRCH_SRNID=PNO102014&_CUR_CMD=InitMulSrch.laf&_CUR_SRNID=PNO102014&_NEXT_CMD=RetrieveRealEstDetailInqSaList.laf' \
           '&_NEXT_SRNID=PNO102018&_PRE_SRNID=&_LOGOUT_CHK=&_FORM_YN=Y'



    response = requests.post(
        'https://www.courtauction.go.kr/RetrieveRealEstDetailInqSaList.laf',
        cookies=cookies,
        headers=headers,
        data=data,
    )
    # contents > table > tbody > tr:nth-child(1) > th:nth-child(1)
    # contents > table > tbody
    # print(response)

    html = response.text  # page_source 얻기
    soup = BeautifulSoup(html, "html.parser")  # get html

    # print(GetLogDef.lineno(__file__), "-------------------------------------------------------------------------------")
    # print(soup)

    rstMainIssueElements = soup.select('#contents > table > tr')

    print(GetLogDef.lineno(__file__), "-------------------------------------------------------------------------------")

    print(GetLogDef.lineno(__file__), "strJiwonName:", strJiwonName)
    print(GetLogDef.lineno(__file__), "strSaYear:", strSaYear)
    print(GetLogDef.lineno(__file__), "strSaSer:", strSaSer)


    # print(rstMainIssueElements)



    nTableLoop = 0

    "https://www.courtauction.go.kr/RetrieveRealEstSaHjosa.laf"

    dictIssue = dict()
    # dictIssue['basement'] = dict()

    for rstMainIssueElement in rstMainIssueElements:
        print(GetLogDef.lineno(__file__), "[", nTableLoop, "]=================================================")

        print(rstMainIssueElement)

        dictIssue[nTableLoop] = dict()

        if nTableLoop == 0:
            getThs = rstMainIssueElement.find_all("th")
            nbbbb = 0
            dictIssue[nTableLoop]['basement'] = dict()
            for getTh in getThs:
                print(GetLogDef.lineno(__file__), "[", nbbbb, "]$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
                print(getTh.getText())
                dictIssue[nTableLoop]['basement'][nbbbb] = dict()
                dictIssue[nTableLoop]['basement'][nbbbb]['title'] =getTh.getText()

                nbbbb = nbbbb + 1



            aaaaas = rstMainIssueElement.find_all("td")
            naaa = 0
            for aaaaa in aaaaas:
                print(GetLogDef.lineno(__file__), "[", naaa, "]###########################################")
                print(aaaaa.getText())

                strDataText = GetLogDef.stripSpecharsForText(aaaaa.getText())

                dictIssue[nTableLoop]['basement'][naaa]['data'] = strDataText

                naaa = naaa + 1


        if nTableLoop == 1:
            print(GetLogDef.lineno(__file__), "[", nTableLoop, "]=================================================")


        # if nTableLoop == 3:
        #     print(rstMainIssueElement.text)
        #     soupd = BeautifulSoup(rstMainIssueElement.text, 'html.parser')  # 👉️ Parsing
        #     # print(soupd.select_one())


        # if nTableLoop == 3:
        #
        #     strContents = rstMainIssueElement.find
        #     # strContents = rstMainIssueElement.getText()
        #     # # strContents = GetLogDef.removeSpechars(strContents).replace("\n", "")
        #     print(GetLogDef.lineno(__file__), "[", nTableLoop, "]####################################################")
        #     print(strContents.rfind("tr"))
        #
        #     # soupd = BeautifulSoup(rstMainIssueElement, "html.parser")  # get html
        #




        nTableLoop = nTableLoop + 1



    # contents > div:nth-child(13)

    rstContents = soup.select("#contents > div")

    rstContents = soup.select("#contents > div.table_contents")

    # rstContents = soup.select("#contents > div:nth-child(9)")

    # rstContents = soup.select("#contents > div:nth-child(9) > table")

    print(dictIssue)




    rstContents = soup.select("#contents > table > tbody")

    "#contents > div.table_title"

    nTableLoop = 0
    print(GetLogDef.lineno(__file__), "-------------------------------------------------------------------------------")

    bIssueListFlag = False
    nIssueListNextNumber = 0

    print(rstContents)


    for rstContent in rstContents:
        print(GetLogDef.lineno(__file__), "["+str(nTableLoop)+"]=================================================")
        nTableLoop = nTableLoop + 1

        # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>", rstContent.getText(), type(rstContent.getText()))

        strContents = rstContent.getText()
        strContents = GetLogDef.removeSpechars(strContents).replace("\n", "")


        if strContents == "사건기본내역":
            bIssueListFlag = True
            nIssueListNextNumber = nTableLoop + 1

        # 사건기본내역
        if bIssueListFlag == True and nTableLoop == nIssueListNextNumber:
            print("ddd")

            # #contents > table > tbody

            # print(rstContent)
            # bIssueListFlag = False



        print(rstContent)



        print(bIssueListFlag , type(bIssueListFlag))
        print(nIssueListNextNumber, type(nIssueListNextNumber))

        #
        # print(rstContent)


        # # 사건기본내역
        # if nTableLoop == 3:
        #
        #     print(rstContent)
        #
        # # 배당요구종기내역
        # if nTableLoop == 6:
        #     print(rstContent)
        #
        # # 항고내역
        # if nTableLoop == 7:
        #     print(rstContent)
        #
        # # 물건내역
        # if nTableLoop == 8:
        #     print(rstContent)
        #
        # # 목록내역
        # if nTableLoop == 9:
        #     print(rstContent)
        #
        # # 당사자 내역
        # if nTableLoop == 10:
        #     print(rstContent)



        # print(rstContent)





    # quit("sssssssssssssssssssssssssss")

    "#contents > div:nth-child(10)"




    # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
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


