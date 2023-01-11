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



# 셀레니엄을 이용한 경매 데이터 크롤링 테스트


try:
    options = webdriver.ChromeOptions()

    options.add_argument('headless')    # 웹 브라우저를 띄우지 않는 headless chrome 옵션 적용
    options.add_argument('disable-gpu')    # GPU 사용 안함
    options.add_argument('lang=ko_KR')    # 언어 설정
    driver = webdriver.Chrome("D:/PythonProjects/chromedriver_win32/chromedriver.exe")

    # driver.get('https://www.courtauction.go.kr/')    # 크롤링할 사이트 호출

    # driver.get('https://m.land.naver.com/map/37.5868152:127.0547066:16/JWJT:DDDGG:SGJT:HOJT/A1?spcMin=155&spcMax=190&')    # 크롤링할 사이트 호출

    ddd = "https://m.land.naver.com/cluster/ajax/articleList?itemId=&mapKey=&lgeo=&showR0=&tradTpCd=A1%3AB1%3AB2&z=13&lat=37.5442087&lon=126.9837139&btm=37.4582755&lft=126.8499897&top=37.6300431&rgt=127.1174381&spcMin=155&spcMax=190&totCnt=23334&sort=dates&page=1"

    aaa = "https://m.land.naver.com/cluster/ajax/articleList?itemId=&mapKey=&lgeo=&showR0=&rletTpCd=APT:OPST:VL:JWJT:DDDGG:SGJT:HOJT:JGB:SG:SMS:GJCG:GM:TJ&tradTpCd=A1:B1:B2&z=12&lat=37.5233133&lon=127.0025967&btm=37.3590776&lft=126.7653606&top=37.6871883&rgt=127.2398327&spcMin=155&spcMax=190&totCnt=21427&cortarNo=&sort=rank&page=1"

    bbb = 'https://m.land.naver.com/cluster/ajax/articleList?itemId=&mapKey=&lgeo=&showR0=&rletTpCd=JWJT:DDDGG:SGJT:HOJT:JGB:SG:GM:TJ&tradTpCd=A1&z=12&lat=37.5385941&lon=126.9793365&btm=37.3677072&lft=126.680474&top=37.7090902&rgt=127.278199&spcMin=155&spcMax=190&totCnt=6053&cortarNo=&sort=rank&page=2'

    ccc = 'https://m.land.naver.com/cluster/ajax/articleList?itemId=&mapKey=&lgeo=&showR0=&rletTpCd=JWJT:DDDGG:SGJT:HOJT:JGB:SG:GM:TJ&tradTpCd=A1&z=12&lat=37.5385941&lon=126.9793365&btm=37.3677072&lft=126.680474&top=37.7090902&rgt=127.278199&spcMin=155&spcMax=190&totCnt=6053&cortarNo=&sort=rank&page=3'

    KuDongDaeMun = 'https://m.land.naver.com/cluster/ajax/articleList?itemId=&mapKey=&lgeo=&showR0=&rletTpCd=APT:OPST:VL:JGC:JWJT:DDDGG:SGJT:HOJT:JGB:SG:SMS:GJCG:GM:TJ:APTHGJ&tradTpCd=A1:B1:B2:B3&z=13&lat=37.574493&lon=127.039765&btm=37.4895481&lft=126.8381487&top=37.6593411&rgt=127.2413813&totCnt=12167&cortarNo=1123000000&sort=dates&page=1'

    KuKangNam = 'https://m.land.naver.com/cluster/ajax/articleList?itemId=&mapKey=&lgeo=&showR0=&rletTpCd=APT:OPST:VL:JGC:JWJT:DDDGG:SGJT:HOJT:JGB:SG:SMS:GJCG:GM:TJ:APTHGJ&tradTpCd=A1:B1:B2:B3&z=12&lat=37.517408&lon=127.047313&btm=37.3472915&lft=126.6440804&top=37.6871376&rgt=127.4505456&totCnt=64858&cortarNo=1168000000&sort=dates&page=1'


    ddddaa = 'https://m.land.naver.com/cluster/ajax/articleList?itemId=&mapKey=&lgeo=&showR0=&rletTpCd=GJCG&tradTpCd=A1:B1:B2:B3&z=12&lat=37.51245&lon=126.9395&btm=37.3412304&lft=126.5360958&top=37.6832777&rgt=127.3429042&totCnt=8&cortarNo=1159000000&sort=dates&page=1'


    aaaa = driver.get(ddddaa)  # 크롤링할 사이트 호출

    html = driver.page_source  # page_source 얻기

    print(html)


    soup = BeautifulSoup(html, "html.parser")  # get html
    print(GetLogDef.lineno(), "====================================================")
    # print(soup)

    elements = soup.select('body')
    ajaxJsonText = elements[0].text

    # print(lineno(), "====================================================")
    # print(ajaxJsonText)


    jsonData = json.loads(ajaxJsonText)

    print(GetLogDef.lineno(), "====================================================")
    print(jsonData)

    jsonArray = (jsonData.get('body'))

    print(GetLogDef.lineno(), "====================================================")
    print(len(jsonArray))

    nLoop=0
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

    for list in jsonArray:

        print(' %i --------------------------------------------------------' % nLoop)
        nLoop += 1

        atclNo = list.get('atclNo')

        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
        qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + "  WHERE  atclNo= %s"



        # #  여기부터 작업질 2022.12.09
        cursorRealEstate.execute(qrySelectNaverMobileMaster, atclNo)

        row_result = cursorRealEstate.rowcount
        if row_result > 0:
            continue

        res = cursorRealEstate.fetchall()
        print(row_result)


        # #make Insert query to kt_realty_naver_mobile_master
        sqlInsertNaverMobileMaster = "INSERT INTO kt_realty_naver_mobile_master SET state='00' "
        dictNaverMobileMaster = {}
        for dictNaverMobileMasterKeys in list.keys():
            dictNaverMobileMaster[dictNaverMobileMasterKeys] = list.get(dictNaverMobileMasterKeys)


            # Non-strings are converted to strings.
            if type(dictNaverMobileMaster[dictNaverMobileMasterKeys]) is not str:
                dictNaverMobileMaster[dictNaverMobileMasterKeys] = str(dictNaverMobileMaster[dictNaverMobileMasterKeys])

            #Json data 'cpLinkVO' is not insert
            if dictNaverMobileMasterKeys != 'cpLinkVO':
                dictNaverMobileMaster[dictNaverMobileMasterKeys]  = dictNaverMobileMaster[dictNaverMobileMasterKeys].replace('\'', ' ')
                sqlInsertNaverMobileMaster = sqlInsertNaverMobileMaster + ', ' + dictNaverMobileMasterKeys + ' = \'' + dictNaverMobileMaster[dictNaverMobileMasterKeys] + '\' '

        aaa = cursorRealEstate.execute(sqlInsertNaverMobileMaster)
        print(aaa)

        print(sqlInsertNaverMobileMaster)

        ResRealEstateConnection.commit();






        # #Do not use the code written below until insert
        # some_dict = {}
        # for SomeDict in list.keys():
        #     # print(SomeDict)
        #     some_dict[SomeDict] = list.get(SomeDict)
        #
        #
        #
        #
        # insertKtSql = "Insert Into kt_realty_naver_mobile_master SET state='00' "
        # for arrDictData in some_dict.keys():
        #
        #     # print(arrDictData)
        #     # print(list.get(arrDictData))
        #     # print(some_dict)
        #     strDictDataValue = list.get(arrDictData)
        #
        #     if type(strDictDataValue) is not str:
        #         strDictDataValue = str(strDictDataValue)
        #
        #     #Json data 'cpLinkVO' is not insert
        #     if arrDictData != 'cpLinkVO':
        #         strDictDataValue = strDictDataValue.replace('\'', ' ')
        #         insertKtSql = insertKtSql + ', ' + arrDictData + ' = \'' + strDictDataValue + '\' '
        # print(insertKtSql)













    # for index, element in enumerate(elements, 1):
    #     print("{} 번째 게시글의 제목: {}".format(index, element.text))

    # print(elements.index())
    # elements2 = elements.select('code')
    # print(elements2)


    # vvvv = requests.get(ddd)  # 크롤링할 사이트 호출
    # print(vvvv.text)

    # print(aaaa)



    driver.quit()    # 크롬 브라우저 닫기

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







