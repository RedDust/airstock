# This is a sample Python script.
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import sys
import json
import pymysql
from collections import OrderedDict
from selenium import webdriver
from urllib.request import Request
from urllib.request import urlopen
import urllib.request
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests
from Init.Functions.Logs import GetLogDef



from Lib.RDB import pyMysqlConnector

from bs4 import BeautifulSoup
from selenium import webdriver    # 라이브러리에서 사용하는 모듈만 호출

from Init.DefConstant import ConstRealEstateTable
from Init.DefConstant import ConstSectorInfo

from Init.Functions.Logs import GetLogDef
# 셀레니엄을 이용한 경매 데이터 크롤링 테스트
import re

try:




    aaa = '📢실사진, 테헤란로 대로변, 초역세권'

    aaa = GetLogDef.rmEmoji(aaa)

    print(aaa)

    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
    qrySelectNaverMobileMaster = "DESC " + ConstRealEstateTable.NaverMobileMasterTable

    cursorRealEstate.execute(qrySelectNaverMobileMaster)
    rows = cursorRealEstate.fetchall()

    for data in rows:
        print(data)


    ResRealEstateConnection.close()
    quit()





    options = webdriver.ChromeOptions()
    options.add_argument('headless')    # 웹 브라우저를 띄우지 않는 headless chrome 옵션 적용
    options.add_argument('disable-gpu')    # GPU 사용 안함
    options.add_argument('lang=ko_KR')    # 언어 설정
    driver = webdriver.Chrome("D:/PythonProjects/chromedriver_win32/chromedriver.exe")

    # driver.get('https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf')    # 크롤링할 사이트 호출


    # html = driver.page_source  # page_source 얻기
    #
    # print(html)

    # driver.get('https://m.land.naver.com/map/37.5868152:127.0547066:16/JWJT:DDDGG:SGJT:HOJT/A1?spcMin=155&spcMax=190&')    # 크롤링할 사이트 호출


    for KuInfo in ConstSectorInfo.dictCortarList:

        cortarNo = ConstSectorInfo.dictCortarList[KuInfo].get('cortarNo')
        tradTpCd = ConstSectorInfo.dictCortarList[KuInfo].get('tradTpCd')
        lat = ConstSectorInfo.dictCortarList[KuInfo].get('lat')
        lon = ConstSectorInfo.dictCortarList[KuInfo].get('lon')
        btm = ConstSectorInfo.dictCortarList[KuInfo].get('btm')
        lft = ConstSectorInfo.dictCortarList[KuInfo].get('lft')
        top = ConstSectorInfo.dictCortarList[KuInfo].get('top')
        rgt = ConstSectorInfo.dictCortarList[KuInfo].get('rgt')
        totCnt = ConstSectorInfo.dictCortarList[KuInfo].get('totCnt')

        # page = ConstSectorInfo.dictCortarList[KuInfo].get('page')




        strCallUrl = ConstSectorInfo.strCallUrl.replace('{%rletTpCd}', ConstSectorInfo.rletTpCd)
        strCallUrl = strCallUrl.replace('{%tradTpCd}', ConstSectorInfo.tradTpCd)
        strCallUrl = strCallUrl.replace('{%cortarNo}', cortarNo)
        strCallUrl = strCallUrl.replace('{%lat}', lat)
        strCallUrl = strCallUrl.replace('{%lon}', lon)
        strCallUrl = strCallUrl.replace('{%btm}', btm)
        strCallUrl = strCallUrl.replace('{%lft}', lft)
        strCallUrl = strCallUrl.replace('{%top}', top)
        strCallUrl = strCallUrl.replace('{%rgt}', rgt)
        strCallUrl = strCallUrl.replace('{%totCnt}', totCnt)



        strCallUrl = strCallUrl.replace('{%page}', '1')
        print(strCallUrl)

        # strCallUrl = strCallUrl.replace('{%page}', page)


        print(lat)

        strCallUrl = 'https://m.land.naver.com/cluster/ajax/articleList?itemId=212211000&mapKey=&lgeo=212211000&showR0=&rletTpCd=TJ&tradTpCd=A1:B1:B2:B3&z=13&lat=37.517408&lon=127.047313&btm=37.4274225&lft=126.8275864&top=37.6072851&rgt=127.2670396&totCnt=46&cortarNo=1168000000&sort=rank&page=3'

        strResult = driver.get(strCallUrl)  # 크롤링할 사이트 호출
        html = driver.page_source  # page_source 얻기
        soup = BeautifulSoup(html, "html.parser")  # get html
        bMore = soup.select('more')
        print(GetLogDef.lineno(), type(bMore))
        print(GetLogDef.lineno(), len(bMore))
        print(GetLogDef.lineno(), type(strCallUrl))


        elements = soup.select('body')
        ajaxJsonText = elements[0].text

        jsonData = json.loads(ajaxJsonText)

        jsonArray = (jsonData.get('body'))

        print(GetLogDef.lineno(), "====================================================")
        print(len(jsonArray))

        if len(jsonArray) < 20:
            break;

        print(jsonData)
        print(GetLogDef.lineno(), "====================================================")

        if bMore == False:
            print(GetLogDef.lineno(), "====================================================")
            break






        driver.quit()    # 크롬 브라우저 닫기


        break







    #
    # html = driver.page_source  # page_source 얻기
    #
    # print(html)
    #
    #
    #
    #


    print("END")

except TypeError:
    print("TypeError")
    raise




# driver.get('https://www.courtauction.go.kr/')    # 크롤링할 사이트 호출
# driver.quit()    # 크롬 브라우저 닫기




# url = "https://www.courtauction.go.kr/RetrieveAucSigu.ajax"
# req = Request(url, headers={'User-Agent':'Mozila/5.0'})
# webpage = urlopen(req)
# soup = BeautifulSoup(webpage)








# Press the green button in the gutter to run the script.







