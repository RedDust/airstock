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

from Init.DefConstant import ConstRealEstateTable
from Init.DefConstant import ConstSectorInfo
from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.SeleniumModule.Windows import Chrome
try:

    switchAtclNo='0000000'
    switchAtclCfmYmd=''
    nInsertedCount = 0

    # options = webdriver.ChromeOptions()
    # options.add_argument("headless")    # 웹 브라우저를 띄우지 않는 headless chrome 옵션 적용
    # options.add_argument("disable-gpu")    # GPU 사용 안함
    # options.add_argument("lang=ko_KR")    # 언어 설정
    # driver = webdriver.Chrome("D:/PythonProjects/chromedriver_win32/chromedriver.exe")

    #크롬 셀리니움 드라이버
    driver = Chrome.defChromeDrive()


    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
    qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterSwitchTable + "  WHERE  type='00'"

    cursorRealEstate.execute(qrySelectNaverMobileMaster)

    results = cursorRealEstate.fetchone()
    nDBSwitchIndex = (results.get('masterCortarIndex'))
    print(results.get('masterCortarName'))
    nDBSwitchPage = results.get('page')
    strDBSwitchResult = results.get('result')
    ResRealEstateConnection.close()


    #서울 부동산 실거래가 데이터 - 임대차
    strProcessType = '010000'
    KuIndex = '00'
    arrCityPlace = '00'
    targetRow = '00'
    # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
    results = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
    nDBSwitchPage = int(results.get('data_3'))
    strDBSwitchResult = results.get('result')

    print(GetLogDef.lineno(__file__), nDBSwitchPage,type(nDBSwitchPage))

    if strDBSwitchResult is False:
        quit(GetLogDef.lineno(__file__), 'strResult => ', strDBSwitchResult)  # 예외를 발생시킴

    if strDBSwitchResult == '10':
        quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strDBSwitchResult)  # 예외를 발생시킴

    #첫실행시 초기화
    if strDBSwitchResult == "00":
        nDBSwitchPage = 0
        nDBSwitchIndex = 1

    date_1 = DateTime.today()
    end_date = date_1 - TimeDelta(days=2)
    nBaseAtclCfmYmd = int(end_date.strftime('%Y%m%d'))
    dtBaseDate = nBaseAtclCfmYmd

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '10'
    dictSwitchData['data_1'] = KuIndex
    dictSwitchData['data_2'] = arrCityPlace
    dictSwitchData['data_3'] = targetRow
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

    # 동별 정보 수집
    for KuIndex, KuInfo in ConstSectorInfo.dictCortarList.items():

        #처리된 구는 패스
        if KuIndex < nDBSwitchIndex:
            # nDBSwitchPage = 0
            continue

        #최근 처리 데이터 세팅
        page = nDBSwitchPage

        cortarNo = ConstSectorInfo.dictCortarList[KuIndex].get('cortarNo')
        cortarName = ConstSectorInfo.dictCortarList[KuIndex].get('cortarName')
        tradTpCd = ConstSectorInfo.dictCortarList[KuIndex].get('tradTpCd')
        lat = ConstSectorInfo.dictCortarList[KuIndex].get('lat')
        lon = ConstSectorInfo.dictCortarList[KuIndex].get('lon')
        btm = ConstSectorInfo.dictCortarList[KuIndex].get('btm')
        lft = ConstSectorInfo.dictCortarList[KuIndex].get('lft')
        top = ConstSectorInfo.dictCortarList[KuIndex].get('top')
        rgt = ConstSectorInfo.dictCortarList[KuIndex].get('rgt')
        totCnt = ConstSectorInfo.dictCortarList[KuIndex].get('totCnt')
        # page = ConstSectorInfo.dictCortarList[KuIndex].get('page')

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


        print(GetLogDef.lineno(), "====================================================")


        # 구 > 페이지별 정보 수집
        while True:    # 무한 루프

            #DB 연결
            nLoop = 0
            print(GetLogDef.lineno(__file__), page ,type(page))
            page = page + 1

            # if page > 1:
            #     break

            print(GetLogDef.lineno(), " page => ", page)

            # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

            #페이지 연산
            print(GetLogDef.lineno(), str(page))
            RealtyCallUrl = strCallUrl.replace('{%page}', str(page))

            #1. 호출
            strResult = driver.get(RealtyCallUrl)  # 크롤링할 사이트 호출
            html = driver.page_source  # page_source 얻기
            soup = BeautifulSoup(html, "html.parser")  # get html

            print(GetLogDef.lineno(), "RealtyCallUrl > ", RealtyCallUrl)


            elements = soup.select('body')


            ajaxJsonText = elements[0].text

            print(GetLogDef.lineno(), "ajaxJsonText > ", ajaxJsonText)

            # bMore = elements.find_all('more')
            # print(GetLogDef.lineno(), ajaxJsonText)
            # print(GetLogDef.lineno(), type(bMore))
            # print(GetLogDef.lineno(), len(bMore))

            jsonData = json.loads(ajaxJsonText)
            bMore = jsonData.get('more')
            jsonArray = (jsonData.get('body'))


            #2. 결과 확인
            if len(jsonArray) < 1:
                nDBSwitchPage = 0
                #처리 할게 없으면 페이지 정보 초기화후 실행중지
                break

            if bMore != True:
                nDBSwitchPage = 0
                #추가데이터가 없으면 페이지 정보 초기화후 처리중지
                break


            #3. 건별 처리
            print("Processing", "====================================================")
            nInsertedCount = 0
            for list in jsonArray:

                print(' [ %i ] ' %nLoop)
                nLoop += 1

                #3-1. 기존에 등록되어 있는 물건인지 확인
                atclNo = list.get('atclNo')
                cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
                qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.NaverMobileMasterTable + " WHERE  atclNo= %s"
                cursorRealEstate.execute(qrySelectNaverMobileMaster, atclNo)
                row_result = cursorRealEstate.rowcount

                #3-2 등록되어 있는 물건이면 패스
                if row_result > 0:
                    continue


                #3-3  중복 아닌 물건은 데이터 저장
                #make Insert query to kt_realty_naver_mobile_master
                sqlInsertNaverMobileMaster = "INSERT INTO " + ConstRealEstateTable.NaverMobileMasterTable + " SET masterCortarNo='"+str(cortarNo)+"' ,masterCortarName='"+cortarName+"'  "
                dictNaverMobileMaster = {}

                for dictNaverMobileMasterKeys in list.keys():
                    dictNaverMobileMaster[dictNaverMobileMasterKeys] = list.get(dictNaverMobileMasterKeys)

                    # Non-strings are converted to strings.
                    if type(dictNaverMobileMaster[dictNaverMobileMasterKeys]) is not str:
                        dictNaverMobileMaster[dictNaverMobileMasterKeys] = str(dictNaverMobileMaster[dictNaverMobileMasterKeys])

                    # Switch를 위한 값 가지고 오기
                    if dictNaverMobileMasterKeys == 'atclCfmYmd':
                        switchAtclCfmYmd = dictNaverMobileMaster[dictNaverMobileMasterKeys]
                        dtBaseYear = "20" + switchAtclCfmYmd[0:2]
                        dtBaseMonth = switchAtclCfmYmd[3:5]
                        dtBaseDay = switchAtclCfmYmd[6:8]
                        #여기 작업 이전 데이터 처리에 연결된 처리 방법
                        dtBaseDate = int(dtBaseYear + dtBaseMonth + dtBaseDay)



                    if dictNaverMobileMasterKeys == 'atclNo':
                        switchAtclNo = dictNaverMobileMaster[dictNaverMobileMasterKeys]




                    # remove special characters For Mysql
                    if dictNaverMobileMasterKeys == 'atclFetrDesc':
                        dictNaverMobileMaster[dictNaverMobileMasterKeys] =GetLogDef.removeSpechars(dictNaverMobileMaster[dictNaverMobileMasterKeys])

                    dictNaverMobileMaster[dictNaverMobileMasterKeys] = dictNaverMobileMaster[dictNaverMobileMasterKeys].replace('\'', ' ')

                    # Json data 'cpLinkVO' is not insert
                    # Insert Processing
                    if dictNaverMobileMasterKeys != 'cpLinkVO':
                        sqlInsertNaverMobileMaster = sqlInsertNaverMobileMaster + ', ' + dictNaverMobileMasterKeys + ' = \'' + \
                                                     dictNaverMobileMaster[dictNaverMobileMasterKeys] + '\' '


                cursorRealEstate.execute(sqlInsertNaverMobileMaster)
                nInsertedCount = nInsertedCount + 1
                print(sqlInsertNaverMobileMaster)

            ResRealEstateConnection.commit()
            print('[ %i ] Inserted' %nInsertedCount)

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = cortarNo
            dictSwitchData['data_2'] = cortarName
            dictSwitchData['data_3'] = KuIndex
            dictSwitchData['data_4'] = page
            dictSwitchData['data_5'] = switchAtclNo
            dictSwitchData['data_6'] = dtBaseDate
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


            #크롤링 딜레이 추가
            nRandomSec = random.randint(3, 5)
            print(GetLogDef.lineno(), "Sleep! " + str(nRandomSec) + " Sec!")
            time.sleep(nRandomSec)

            # if len(jsonArray) < 20:
            #     break


            ResRealEstateConnection.close()
            print("while True END", "====================================================")

            # 기존에 처리 했던 날짜 이면 브레이크
            print(nBaseAtclCfmYmd, ">", dtBaseDate)
            if nBaseAtclCfmYmd > dtBaseDate:
                nDBSwitchPage = 0
                print(GetLogDef.lineno(), nBaseAtclCfmYmd, " > ", dtBaseDate)
                break


        #페이지 정보 초기화
        nDBSwitchPage = 0
        print("KuInfo END", "====================================================")

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_6'] = nInsertedCount
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)




except Exception as e:

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '20'
    dictSwitchData['data_6'] = nInsertedCount
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    print("Error Exception")
    print(e)
    print(type(e))

    print(sqlInsertNaverMobileMaster)


else:
    print("========================================================")

finally:
    driver.quit()    # 크롬 브라우저 닫기
    print("Finally END")


