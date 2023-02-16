# 셀레니움으로 만든 크롤링
# 페이징 오류로 인해 셀레니움으로 진행이 어렵다고 판단됨
# CURL 버전으로 새로 개발하기로함


quit("CURL 버전으로 새로 개발하기로함")



import requests


# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import datetime
sys.path.append("/")

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Init.DefConstant import ConstRealEstateTable
from Init.DefConstant import ConstSectorInfo
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Realty.Auction.Const import AuctionCourtInfo

try:


    #크롬 셀리니움 드라이버
    driver = Chrome.defChromeDrive()

    strCourtAuctionCallUrl  =   "https://www.courtauction.go.kr/"

    strResult = driver.get(strCourtAuctionCallUrl)  # 크롤링할 사이트 호출

    print(strResult)

    # if strResult.status_code != 200:
    #     Exception(GetLogDef.lineno(), 'strResult.status_code => ', strResult.status_code)  # 예외를 발생시킴




    #1. 메인페이지에서 경매 조회 버튼 클릭 -> 물건 상세 페이지
    driver.switch_to.frame('indexFrame')
    Selected = Select(driver.find_element_by_name('mDaepyoSidoCd'))
    Selected.select_by_index(1)  # select index value
    bbb = driver.find_element_by_id("main_btn").click()


    # #2. 물건상세페이지 -> 매각기일 정렬
    # strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[1]/table/thead/tr/th[7]/a[1]"
    # objOrderButton = driver.find_element_by_xpath(strOrderXPath)
    # objOrderButton.click()
    #
    # strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[1]/table/thead/tr/th[7]/a[1]"
    # objOrderButton = driver.find_element_by_xpath(strOrderXPath)
    # objOrderButton.click()


    # print(rstMainElements)

    nLoopTotal = 0
    while True:

        html = driver.page_source  # page_source 얻기
        soup = BeautifulSoup(html, "html.parser")  # get html
        rstMainElements = soup.select_one('#contents > div.table_contents > form:nth-child(1) > table > tbody')

        nLoopTrElements = 0
        rstTrElements = rstMainElements.select('tr')
        for rstTrElement in rstTrElements:
            print(GetLogDef.lineno(__file__), "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            nLoopTrElements = nLoopTrElements + 1

            nLoopTdElements = 0
            rstTdElements = rstTrElement.select('td')


            # 법원경매 0번째 컬럼 (CHECKBOX - 법정,고유코드,) START
            strCheckBoxValues = (rstTdElements[0].select_one('input[type=checkbox]').get('value'))
            arrCheckBoxValues = strCheckBoxValues.split(',')
            if arrCheckBoxValues != 3:
                Exception(GetLogDef.lineno(__file__), 'arrCheckBoxValues => ', arrCheckBoxValues)  # 예외를 발생시킴

            print(arrCheckBoxValues)
            strCourtName = arrCheckBoxValues[0]
            strAuctionUniqueNumber = arrCheckBoxValues[1]
            strAuctionSeq = arrCheckBoxValues[2]
            # 법원경매 0번째 컬럼 (CHECKBOX) END




            # 법원경매 1번째 컬럼 (사건번호) START
            rstIssueNumber = rstTdElements[1]
            print(GetLogDef.lineno(__file__), "-------------------------------------------------------------------------------")
            print(rstIssueNumber)
            rstIssueNumber.find('input').decompose()
            strIssueNumber = rstIssueNumber.text
            nLoopTempIssueNumber = 0
            arrTempIssueNumber = []
            arrIssueNumbers = strIssueNumber.split("\n")
            for arrIssueNumber in arrIssueNumbers:
                strTempIssueNumber = arrIssueNumber.replace('\t', '')
                strTempIssueNumber = strTempIssueNumber.replace("\'", "")
                if len(strTempIssueNumber) > 0:
                    arrTempIssueNumber.append(strTempIssueNumber)
                    strTempIssueNumber = nLoopTempIssueNumber + 1

            #법원 정보가 존재 하지 않으면 오류 처리
            if arrTempIssueNumber[0] not in AuctionCourtInfo.arrCourtName:
                Exception(GetLogDef.lineno(__file__), 'arrTempIssueNumber[0] => ', arrTempIssueNumber[0])  # 예외를 발생시킴

            #사건 번호 정보가 없으면 오류
            if len(arrTempIssueNumber) < 2:
                Exception(GetLogDef.lineno(__file__), 'len(arrTempIssueNumber) => ', len(arrTempIssueNumber))  # 예외를 발생시킴

            # #중복 물건 또는 병합물건 존재함
            # if len(arrTempIssueNumber) > 2:
            #     print("어레이가 2개 이상", len(arrTempIssueNumber))

            jsonIssueNumber = json.dumps(arrTempIssueNumber, ensure_ascii=False)
            print(jsonIssueNumber)
            # jsonIssueNumber
            # 법원경매 1번째 컬럼 (사건번호) END



            # 법원경매 2번째 컬럼 (용도번호) START
            # print(GetLogDef.lineno(__file__), "================================================================================")
            rstUsage = rstTdElements[2]
            arrUsages = str(rstUsage).split("<br/>")
            nLoopTempUsageInfo = 0
            arrTempUsageInfo = []
            for arrUsage in arrUsages:
                strTempUsage = repr(arrUsage)
                strTempUsage = strTempUsage.replace('<td>', '')
                strTempUsage = strTempUsage.replace("\'", "")
                strTempUsage = strTempUsage.replace("\\n", "")
                strTempUsage = strTempUsage.replace("\\t", "")
                strTempUsage = strTempUsage.replace('</td>', '')
                if len(strTempUsage) > 0:
                    arrTempUsageInfo.append(strTempUsage)
                    nLoopTempUsageInfo = nLoopTempUsageInfo + 1

            #사건 번호 정보가 없으면 오류
            if len(arrTempUsageInfo) < 2:
                Exception(GetLogDef.lineno(__file__), 'len(arrTempIssueNumber) => ', len(arrTempUsageInfo))  # 예외를 발생시킴

            # #중복 물건 또는 병합물건 존재함
            # if len(arrTempUsageInfo) > 2:
            #     print("어레이가 2개 이상", len(arrTempUsageInfo))

            print(arrTempUsageInfo)
            #["'1'", "'대지'", "'임야'", "'전답'"]

            # 법원경매 2번째 컬럼 (용도번호) END



            # 법원경매 3번째 컬럼 (소재지 및 내역) START
            # print(GetLogDef.lineno(__file__), "================================================================================")
            rstAddressAndContents = rstTdElements[3]
            # print(rstAddressAndContents)
            nLoopTempAddressInfo = 0
            arrTempAddressInfo = []
            arrayAtags = rstAddressAndContents.select('a')
            for arrayAtag in arrayAtags:
                # print(GetLogDef.lineno(__file__),"-------------------------------------------------------------------------------")
                strTempAddress = repr(arrayAtag.text)
                strTempAddress = strTempAddress.replace("\'", "")
                strTempAddress = strTempAddress.replace("\\n", "")
                strTempAddress = strTempAddress.replace("\\t", "")
                if len(strTempAddress) < 1:
                    continue

                arrTempAddressInfo.append(strTempAddress)
                nLoopTempAddressInfo = nLoopTempAddressInfo + 1

            print(arrTempAddressInfo)
            #['서울특별시 관악구 신림동  103-260 ', '서울특별시 관악구  복은6길 20-4 ']
            # 법원경매 3번째 컬럼 (소재지 및 내역) END




            # 법원경매 4번째 컬럼 (비고) START
            rstEtcContents = rstTdElements[4]
            strTempAddress = repr(rstEtcContents.text).replace("\\t", "")
            print(strTempAddress)
            # 법원경매 4번째 컬럼 (비고) END


            # 법원경매 5번째 컬럼 (감정평가액 / 최처매각가격) START
            # print(GetLogDef.lineno(__file__), "================================================================================")
            rstAuctionCosts = rstTdElements[5]
            rstDivs = rstAuctionCosts.select('div')
            nLoopTempAddressInfo = 0
            arrActionCustInfo = []
            for rstDiv in rstDivs:
                # print(GetLogDef.lineno(__file__),"################################################################")
                strTempAuctionCosts = repr(rstDiv.text)
                strTempAuctionCosts = strTempAuctionCosts.split('\\n')

                for strAuctionCost in strTempAuctionCosts:
                    strTempAuctionCost = strAuctionCost.replace("\'", "")
                    strTempAuctionCost = strTempAuctionCost.replace("\\n", "")
                    strTempAuctionCost = strTempAuctionCost.replace("\\t", "")
                    strTempAuctionCost = strTempAuctionCost.replace("(", "")
                    strTempAuctionCost = strTempAuctionCost.replace(")", "")
                    strTempAuctionCost = strTempAuctionCost.replace("%", "")
                    strTempAuctionCost = strTempAuctionCost.replace(",", "")
                    if len(strTempAuctionCost) > 0:
                        arrActionCustInfo.append(strTempAuctionCost)

            print(arrActionCustInfo)
            #['277000000', '113459000', '40']
            # 법원경매 5번째 컬럼 (감정평가액 / 최처매각가격) END


            # 법원경매 6번째 컬럼 (담당계 / 매각기일) START
            # print(GetLogDef.lineno(__file__), "================================================================================")
            rstShowJpDeptInfoTitleInfos = rstTdElements[6]
            rstShowJpDeptInfoTitles = repr(rstShowJpDeptInfoTitleInfos.text)
            rstShowJpDeptInfoTitlesArrays = rstShowJpDeptInfoTitles.split('\\n')

            arrShowJpDeptInfoTitle = []
            for rstShowJpDeptInfoTitlesArray in rstShowJpDeptInfoTitlesArrays:
                rstShowJpDeptInfoTitlesArray = rstShowJpDeptInfoTitlesArray.replace("\'", "")
                rstShowJpDeptInfoTitlesArray = rstShowJpDeptInfoTitlesArray.replace("\\t", "")
                rstShowJpDeptInfoTitleText = rstShowJpDeptInfoTitlesArray.replace(" ", "")

                if len(rstShowJpDeptInfoTitleText) > 0:
                    # print(rstShowJpDeptInfoTitleText)
                    arrShowJpDeptInfoTitle.append(rstShowJpDeptInfoTitleText)

            # aTextInfo = rstShowJpDeptInfoTitleInfos.select_one('a').get('onclick')
            # print(aTextInfo)

            print(arrShowJpDeptInfoTitle)
            #['경매21계', '2023.02.21', '유찰3회']
            # 법원경매 6번째 컬럼 (감정평가액 / 최처매각가격) END



        # 테스트 딜레이 추가
        nRandomSec = random.randint(2, 3)
        print(GetLogDef.lineno(), "Sleep! " + str(nRandomSec) + " Sec!")
        time.sleep(nRandomSec)

        # strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[2]/div/div[1]/a[1]"
        # objOrderButton = driver.find_element_by_xpath(strOrderXPath).click()

        nLoopTotal = nLoopTotal + 1
        strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[2]/div/div[1]/a["+str(nLoopTotal)+"]"
        objOrderButton = driver.find_element_by_xpath(strOrderXPath).click()

        nRandomSec = random.randint(3, 4)
        print(GetLogDef.lineno(), "Sleep! " + str(nRandomSec) + " Sec!")
        time.sleep(nRandomSec)

        print(objOrderButton)










    #END While





    # soup.select_one('#contents > div.table_contents > form:nth-child(2) > div > div.page2 > a:nth-child(2)')

    # strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[2]/div/div[1]/a[1]"
    # objOrderButton = driver.find_element_by_xpath(strOrderXPath).click()

    # strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[2]/div/div[1]/a[2]"
    # objOrderButton = driver.find_element_by_xpath(strOrderXPath)


    # strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[2]/div/div[1]/a[3]"
    # objOrderButton = driver.find_element_by_xpath(strOrderXPath)

    # strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[2]/div/div[1]/a[9]"
    # objOrderButton = driver.find_element_by_xpath(strOrderXPath)

    # strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[2]/div/div[1]/a[10]"
    # objOrderButton = driver.find_element_by_xpath(strOrderXPath)



    # strOrderXPath = "/html/body/div[1]/div[4]/div[3]/div[4]/form[2]/div/div[1]/a[10]/img"
    # objOrderButton = driver.find_element_by_xpath(strOrderXPath)




    # '#contents > div.table_contents > form:nth-child(2) > div > div.page2 > a:nth-child(3)'
    #
    # '#contents > div.table_contents > form:nth-child(2) > div > div.page2 > a:nth-child(4)'
    #
    # soup.select_one('#contents > div.table_contents > form:nth-child(1) > table > tbody')


    print(GetLogDef.lineno(__file__), "-------------------------------------------------------------------------------")
    print("echod")




    # vvvv = elements.select('form')

    # print(vvvv)

    # time.sleep("10")
    #
    #
    #
    # driver.find_element_by_name('mDaepyoSidoCd').click()
    # # option = driver.find_element_by_xpath("//*[text()='검색값']")
    # # driver.execute_script("arguments[0].scrollIntoView();", option)
    #
    #


    # driver.find_element_by_class_name('log_btn').click()

    # # 크롤링 딜레이 추가
    # nRandomSec = random.randint(1, 3)
    # print(GetLogDef.lineno(), "Sleep! " + str(nRandomSec) + " Sec!")
    # time.sleep(nRandomSec)


    # strResult = driver.get("https://www.ppomppu.co.kr/zboard/view.php?id=humor&page=1&divpage=100&no=550017")  # 크롤링할 사이트 호출
    #
    # # 크롤링 딜레이 추가
    # nRandomSec = random.randint(1, 3)
    # print(GetLogDef.lineno(), "Sleep! " + str(nRandomSec) + " Sec!")
    # time.sleep(nRandomSec)
    #
    # strCourtAuctionCallUrl = "https://www.ppomppu.co.kr/zboard/login.php?r_url=https%3A%2F%2Fwww.ppomppu.co.kr%2Fzboard%2Fview.php%3Fid%3Dhumor%26page%3D1%26divpage%3D100%26no%3D550017"
    #
    #
    # strResult = driver.get(strCourtAuctionCallUrl)  # 크롤링할 사이트 호출
    #
    #
    # driver.find_element_by_name('user_id').send_keys('reddust')
    # driver.find_element_by_name('password').send_keys('QhaQn!@84')
    #
    # driver.find_element_by_class_name('log_btn').click()
    #
    #



    # strCourtAuctionCallUrl = "https://www.courtauction.go.kr/RetrieveRealEstMulDetailList.laf"
    #
    # PostData = "page=default20&page=default20&srnID=PNO102000&jiwonNm=&bubwLocGubun=2&jibhgwanOffMgakPlcGubun=&mvmPlaceSidoCd=&mvmPlaceSiguCd=&roadPlaceSidoCd=&roadPlaceSiguCd=&daepyoSidoCd=11&daepyoSiguCd=&daepyoDongCd=&rd1Cd=&rd2Cd=&rd3Rd4Cd=&roadCode=&notifyLoc=1&notifyRealRoad=1&notifyNewLoc=1&mvRealGbncd=1&jiwonNm1=%C0%FC%C3%BC&jiwonNm2=%BC%AD%BF%EF%C1%DF%BE%D3%C1%F6%B9%E6%B9%FD%BF%F8&mDaepyoSidoCd=11&mvDaepyoSidoCd=&mDaepyoSiguCd=&mvDaepyoSiguCd=&realVowel=00000_55203&vowelSel=00000_55203&mDaepyoDongCd=&mvmPlaceDongCd=&_NAVI_CMD=&_NAVI_SRNID=&_SRCH_SRNID=PNO102000&_CUR_CMD=RetrieveMainInfo.laf&_CUR_SRNID=PNO102000&_NEXT_CMD=&_NEXT_SRNID=PNO102002&_PRE_SRNID=PNO102001&_LOGOUT_CHK=&_FORM_YN=Y&PNIPassMsg=%C1%A4%C3%A5%BF%A1+%C0%C7%C7%D8+%C2%F7%B4%DC%B5%C8+%C7%D8%BF%DCIP+%BB%E7%BF%EB%C0%DA%C0%D4%B4%CF%B4%D9.&pageSpec=default20&pageSpec=default20&targetRow=61&lafjOrderBy="


    # strResult = driver.post(strCourtAuctionCallUrl, PostData)  # 크롤링할 사이트 호출





    print(strResult)

except Exception as e:
    print("Error Exception")
    print(e)
    print(type(e))

else:
    print(GetLogDef.lineno(__file__), "============================================================")

finally:
    # driver.quit()    # 크롬 브라우저 닫기
    print("Finally END")


