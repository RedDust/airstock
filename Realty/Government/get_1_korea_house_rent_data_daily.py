import sys
sys.path.append("D:/PythonProjects/airstock")

# https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15058017
#국토부 오피스텔 실거래
#SERVICE URL https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15058017
#[국토부 단독주택 실거래 자료]API URL http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcSHRent

#ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable = 'kt_realty_gov_code_info'
#ConstRealEstateTable_GOV.MolitTownHouseRealRentMasterTable = 'kt_realty_molit_townhouse_real_rent_master'


import urllib.request
import json
import pymysql
import traceback
import time
import re
import pandas as pd
import requests
from Realty.Government.Init import init_conf
from Lib.RDB import pyMysqlConnector
from dateutil.relativedelta import relativedelta


from Realty.Government.Const import ConstRealEstateTable_GOV
from Init.DefConstant import ConstRealEstateTable
import inspect
import logging
import logging.handlers
from Init.Functions.Logs import GetLogDef

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
from bs4 import BeautifulSoup
import xml
import xml.etree.ElementTree as ET

from Lib.CustomException import QuitException
def main():

    try:

        #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
        #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
        #거래 신고 30일 + 취소 신고 +30일
        stToday = DateTime.today()

        nInsertedCount = 0
        nUpdateCount = 0

        #서울 부동산 실거래가 데이터 - 매매
        strProcessType = '035221'
        HOUSE_TYPE = '단독다가구'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'
        nProcessCount=0
        dtProcessDay=''
        strAdminSection=''
        # # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        strSwitchSidoCode=''
        strAdminName=''
        strGOVMoltyAddressSequence='0'
        intLoopStart=0
        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            QuitException.QuitException(GetLogDef.lineno(__file__)+ 'strResult => '+ strResult)  # 예외를 발생시킴

        if strResult == '10':
            QuitException.QuitException(GetLogDef.lineno(__file__)+ 'It is currently in operation. => '+ strResult)  # 예외를 발생시킴

        if strResult == '20':
            strGOVMoltyAddressSequence = str(rstResult.get('data_3'))


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_5'] = nUpdateCount
        dictSwitchData['data_6'] = nInsertedCount
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


        qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
        qrySelectSeoulTradeMaster += " WHERE state='00' AND sgg_cd<>'000' AND umd_cd='000' AND ri_cd='00'"
        qrySelectSeoulTradeMaster += " AND seq >= "+strGOVMoltyAddressSequence+" "
        qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
        # qrySelectSeoulTradeMaster += " LIMIT 1 "

        cursorRealEstate.execute(qrySelectSeoulTradeMaster)
        row_result = cursorRealEstate.rowcount
        # 등록되어 있는 물건이면 패스


        print(GetLogDef.lineno(__file__), "row_result >> " , row_result)
        rstSelectDatas = cursorRealEstate.fetchall()

        intRangeStart = int(intLoopStart)
        intRangeEnd = 3
        for rstSelectData in rstSelectDatas:

            strGOVMoltyAddressSequence = str(rstSelectData.get('seq'))
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "strGOVMoltyAddressSequence >> ", strGOVMoltyAddressSequence)

            sido_code = str(rstSelectData.get('sido_cd')).zfill(2)
            sigu_code = rstSelectData.get('sgg_cd').zfill(3)

            strAdminName = str(rstSelectData.get('locatadd_nm'))

            strAdminSection =  sido_code+sigu_code

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "strAdminName >> ", strAdminName)
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "strAdminSection >> ", strAdminSection)

            dtToday = DateTime.now()

            #시작월 마지막 월 (12개월 * 30년)

            for nLoop in range(intRangeStart, intRangeEnd):
                # for nLoop in range(0, 730):

                print(GetLogDef.lineno(__file__), "[START LOOP]]================== ", nLoop)

                nbaseDate = dtToday - relativedelta(months=nLoop)
                dtProcessDay = str(int(nbaseDate.strftime("%Y%m")))

                print(GetLogDef.lineno(__file__), "dtProcessDay >> ", dtProcessDay)

                url = 'https://apis.data.go.kr/1613000/RTMSDataSvcSHRent/getRTMSDataSvcSHRent'

                params = {'serviceKey': init_conf.MolitDecodedAuthorizationKey, 'LAWD_CD': strAdminSection,'DEAL_YMD': str(dtProcessDay)}
                # print("url===> ", strAdminSection, dtProcessDay, url)
                # response = requests.get(url, params=params)
                # print("response===> ", type(response),  response )
                # print("response.status_code===> ", type(response.status_code), response.status_code)
                # print("response.raise_for_status()===> ", type(response.raise_for_status()), response.raise_for_status())

                # if response.status_code != 200:
                while True:
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno),
                          "============================time.sleep(1) ")
                    time.sleep(1)
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno),
                          "============================time.sleep(1) PASS ")
                    try:

                        response = requests.get(url, params=params)
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(inspect.currentframe()).lineno), "response===> ",
                              type(response), response)
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(inspect.currentframe()).lineno),
                              "response.status_code===> ", type(response.status_code), response.status_code)
                        if response.status_code == int(200):
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                    inspect.getframeinfo(inspect.currentframe()).lineno), "break ",
                                  type(response.raise_for_status()), response.raise_for_status())
                            responseContents = response.text  # page_source 얻기
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                    inspect.getframeinfo(
                                                        inspect.currentframe()).lineno) + "responseContents===> ",
                                  type(responseContents), responseContents)
                            ElementResponseRoot = ET.fromstring(responseContents)
                            # print("ElementResponseRoot===> ", type(ElementResponseRoot),  ElementResponseRoot, )
                            strHeaderResultCode = ElementResponseRoot.find('header').find('resultCode').text
                            strHeaderResultMessage = ElementResponseRoot.find('header').find('resultMsg').text
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                    inspect.getframeinfo(
                                                        inspect.currentframe()).lineno) + "strHeaderResultCode===> ",
                                  type(strHeaderResultCode), strHeaderResultCode)
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                    inspect.getframeinfo(
                                                        inspect.currentframe()).lineno) + "strHeaderResultMessage===> ",
                                  type(strHeaderResultMessage), strHeaderResultMessage)
                            if strHeaderResultCode == '000':
                                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                        inspect.getframeinfo(
                                                            inspect.currentframe()).lineno) + "url===> ", type(url),
                                      url)
                                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                        inspect.getframeinfo(
                                                            inspect.currentframe()).lineno) + "params===> ",
                                      type(params), params)
                                break

                    except requests.exceptions.Timeout as e:
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.Timeout  url===> ",
                              type(url), url)
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.Timeout params===> ",
                              type(params), params)
                        time.sleep(10)

                    except requests.exceptions.ConnectionError as errc:
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.ConnectionError  url===> ",
                              type(url), url)
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.ConnectionError params===> ",
                              type(params), params)
                        time.sleep(10)
                    except requests.exceptions.HTTPError as errb:
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.HTTPError  url===> ",
                              type(url), url)
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.HTTPError params===> ",
                              type(params), params)
                        time.sleep(10)

                    # Any Error except upper exception
                    except requests.exceptions.RequestException as erra:
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.RequestException  url===> ",
                              type(url), url)
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.RequestException params===> ",
                              type(params), params)
                        time.sleep(10)

                    except Exception as e:

                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.Exception  url===> ",
                              type(url), url)
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                inspect.getframeinfo(
                                                    inspect.currentframe()).lineno) + "requests.exceptions.Exception params===> ",
                              type(params), params)
                        time.sleep(10)



                objectBodyItemAll = ElementResponseRoot.find('body').find('items')
                print(GetLogDef.lineno(__file__), "objectBodyItemAllCount >> ", len(objectBodyItemAll))
                # intGetCount = len(objectBodyItemAll)
                for objectBodyItem in objectBodyItemAll:
                    print(GetLogDef.lineno(__file__), "================objectBodyItem >> ", len(objectBodyItem) , objectBodyItem.tag)
                    print(GetLogDef.lineno(__file__), "================Text >> ", type( objectBodyItemAll.iter()) ,  objectBodyItemAll.iter())

                    for aaa in objectBodyItem.iter():
                        print(GetLogDef.lineno(__file__), "objectBodyItem.iter() >> ", aaa.tag ," =>" , aaa.text)

                    # print(objectBodyItem.find('거래금액').text)
                    OBJ_AMT='0'
                    REQ_GBN=''
                    BUILD_YEAR=''
                    strTradeYYYY=''
                    strTradeMM=''
                    strTradeDD=''
                    BLDG_DONG=''
                    REGISTER_YMD=''
                    SELLER=''
                    BUYER=''
                    BJDONG_NM=''
                    BLDG_NM=''
                    TOT_AREA=''
                    AGENT_ADDR=''
                    BJD_JIUN=''
                    SGG_CD=''
                    FLOOR=''
                    CONTRACT_START=''
                    CONTRACT_END = ''
                    PREVIOUS_RENT_AMT='0'

                    if objectBodyItem.find('buildYear') != None:
                        BUILD_YEAR = str(objectBodyItem.find('buildYear').text).strip()
                    if objectBodyItem.find('contractType') != None:
                        REQ_GBN = str(objectBodyItem.find('contractType').text).strip()
                    if objectBodyItem.find('contractTerm') != None:
                        CONTRACT_TERM = str(objectBodyItem.find('contractTerm').text).strip()
                    if objectBodyItem.find('dealYear') != None:
                        strTradeYYYY = str(objectBodyItem.find('dealYear').text).strip().zfill(4)
                    if objectBodyItem.find('dealMonth') != None:
                        strTradeMM = str(objectBodyItem.find('dealMonth').text).strip().zfill(2)
                    if objectBodyItem.find('dealDay') != None:
                        strTradeDD = str(objectBodyItem.find('dealDay').text).strip().zfill(2)
                    if objectBodyItem.find('umdNm') != None:
                        BJDONG_NM = str(objectBodyItem.find('umdNm').text).strip()
                    if objectBodyItem.find('houseType') != None:
                        BLDG_NM = str(objectBodyItem.find('houseType').text).strip().replace('\'',"")
                    if objectBodyItem.find('totalFloorAr') != None:
                        TOT_AREA = str(objectBodyItem.find('totalFloorAr').text).strip()
                    if objectBodyItem.find('deposit') != None:
                        DEPOSIT_AMT = str(objectBodyItem.find('deposit').text).strip().replace(",", "")
                    if objectBodyItem.find('monthlyRent') != None:
                        RENT_AMT = str(objectBodyItem.find('monthlyRent').text).strip().replace(",", "")
                    if objectBodyItem.find('preDeposit') != None:
                        PREVIOUS_DEPOSIT_AMT = str(objectBodyItem.find('preDeposit').text).strip().replace(",", "")
                    if objectBodyItem.find('preMonthlyRent') != None:
                        PREVIOUS_RENT_AMT = str(objectBodyItem.find('preMonthlyRent').text).strip().replace(",", "")
                    if objectBodyItem.find('useRRRight') != None:
                        RENEWAL_CONTRACT = str(objectBodyItem.find('useRRRight').text).strip()
                    if objectBodyItem.find('sggCd') != None:
                        SGG_CD = str(objectBodyItem.find('sggCd').text).strip()

                    if len(DEPOSIT_AMT) < 1 or DEPOSIT_AMT == None  or DEPOSIT_AMT== 'None':
                        DEPOSIT_AMT = '0'
                    if len(RENT_AMT) < 1 or RENT_AMT == None  or RENT_AMT== 'None':
                        RENT_AMT = '0'
                    if len(PREVIOUS_DEPOSIT_AMT) < 1 or PREVIOUS_DEPOSIT_AMT == None or PREVIOUS_DEPOSIT_AMT== 'None':
                        PREVIOUS_DEPOSIT_AMT = '0'
                    if len(PREVIOUS_RENT_AMT) < 1  or PREVIOUS_RENT_AMT == None or PREVIOUS_RENT_AMT== 'None':
                        PREVIOUS_RENT_AMT = '0'

                    DEAL_YMD = strTradeYYYY + strTradeMM + strTradeDD


                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno),"BJDONG_NM" , "["+BJDONG_NM+"]")
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno), "SGG_CD", "[" + SGG_CD + "]")

                    sido_code = SGG_CD[0:2].zfill(2)
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno), "SGG_CD", "[" + SGG_CD + "]")
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno), "sido_code", "[" + sido_code + "]")

                    sigu_code = SGG_CD[2:5].zfill(3)
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno), "SGG_CD", "[" + SGG_CD + "]")
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno), "sigu_code", "[" + sigu_code + "]")


                    print(GetLogDef.lineno(__file__), "BJD_JIUN =====> ", BJD_JIUN)

                    listBJD_JIUN = BJD_JIUN.split("-")
                    print(GetLogDef.lineno(__file__), "listBJD_JIUN =====> ", listBJD_JIUN , len(listBJD_JIUN) , type(listBJD_JIUN))
                    if len(listBJD_JIUN) == 1:
                        BONBEON = re.sub(r'[^0-9]', '', listBJD_JIUN[0]).zfill(4)
                        BUBEON = '0000'
                        print(GetLogDef.lineno(__file__), "BONBEON=====> ", BONBEON, len(BONBEON),type(BONBEON))

                    elif len(listBJD_JIUN) == 2:
                        BONBEON = re.sub(r'[^0-9]', '', listBJD_JIUN[0]).zfill(4)
                        BUBEON = re.sub(r'[^0-9]', '', listBJD_JIUN[1]).zfill(4)
                        print(GetLogDef.lineno(__file__), "BONBEON=====> ", BONBEON, len(BONBEON), type(BONBEON))

                    elif len(listBJD_JIUN) > 2:
                        listTemp = []
                        for intTempLoop in range(len(listBJD_JIUN)):
                            print(GetLogDef.lineno(__file__), "intTempLoop=====> ", intTempLoop, type(intTempLoop))
                            strTemp = re.sub(r'[^0-9]', '', listBJD_JIUN[intTempLoop])

                            print(GetLogDef.lineno(__file__), "strTemp=====> ", strTemp, type(strTemp))

                            if len(strTemp) > 0:
                                listTemp.append(strTemp)

                        print(GetLogDef.lineno(__file__), "listTemp=====> ", listTemp, len(listTemp), type(listTemp))

                        if len(listTemp) == 1:
                            print(GetLogDef.lineno(__file__), "BONBEON=====> ", BONBEON, len(BONBEON), type(BONBEON))
                            BONBEON = re.sub(r'[^0-9]', '', listTemp[0]).zfill(4)
                            BUBEON = '0000'
                        elif len(listTemp) == 2:
                            print(GetLogDef.lineno(__file__), "listTemp=====> ", listTemp, len(listTemp), type(listTemp))
                            BONBEON = re.sub(r'[^0-9]', '', listTemp[0]).zfill(4)
                            BUBEON = re.sub(r'[^0-9]', '', listTemp[1]).zfill(4)

                        elif len(listTemp) > 2:
                            print(GetLogDef.lineno(__file__), "listTemp =====> ", listTemp)
                            BONBEON = re.sub(r'[^0-9]', '', listTemp[0]).zfill(4)
                            BUBEON = re.sub(r'[^0-9]', '', listTemp[1]).zfill(4)
                    else:
                        raise Exception("listBJD_JIUN => " + str(listBJD_JIUN))


                    listCONTRACT_TERMs = CONTRACT_TERM.split("~")
                    if len(listCONTRACT_TERMs) == 2:
                        CONTRACT_START = listCONTRACT_TERMs[0]
                        CONTRACT_END = listCONTRACT_TERMs[1]
                    else:
                        CONTRACT_START = CONTRACT_TERM

                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno),"BJDONG_NM" , "["+BJDONG_NM+"]")

                    sqlSelectGOVCodeinfo  = " SELECT * FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
                    sqlSelectGOVCodeinfo += " WHERE sido_cd='"+sido_code+"' AND sgg_cd='"+sigu_code+"' "
                    sqlSelectGOVCodeinfo += " AND locatadd_nm LIKE '% " + BJDONG_NM + "' "
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "sqlSelectGOVCodeinfo =====> "+ sqlSelectGOVCodeinfo +sido_code + sigu_code )
                    cursorRealEstate.execute(sqlSelectGOVCodeinfo)
                    intGovCodeCount = cursorRealEstate.rowcount

                    if intGovCodeCount != 1:
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "sqlSelectGOVCodeinfo =====> ", sqlSelectGOVCodeinfo)

                        sqlSelectGOVCodeinfo = " SELECT * FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
                        sqlSelectGOVCodeinfo += " WHERE sido_cd='" + sido_code + "' AND sgg_cd='" + sigu_code + "' "
                        sqlSelectGOVCodeinfo += " AND replace(locatadd_nm,' ' ,'') LIKE '%" + BJDONG_NM.replace(" ","") + "' "
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(
                                                           inspect.currentframe()).lineno) + "sqlSelectGOVCodeinfo =====> " + sqlSelectGOVCodeinfo + sido_code + sigu_code)
                        cursorRealEstate.execute(sqlSelectGOVCodeinfo)
                        intGovCodeCount = cursorRealEstate.rowcount

                        if intGovCodeCount != 1:
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                    inspect.getframeinfo(inspect.currentframe()).lineno),
                                  "sqlSelectGOVCodeinfo =====> ", sqlSelectGOVCodeinfo)
                            raise Exception("intGovCodeCount => " + str(intGovCodeCount))


                    else:
                        rstSelectDatas = cursorRealEstate.fetchone()
                        BJDONG_CD = rstSelectDatas.get('umd_cd') + rstSelectDatas.get('ri_cd')
                        BJDONG_NM = rstSelectDatas.get('umd_nm') + " " + rstSelectDatas.get('ri_nm')
                        SIDO_NM = rstSelectDatas.get('sido_nm')
                        SGG_NM = rstSelectDatas.get('sgg_nm')


                    if len(BJDONG_CD) < 5:
                        print(GetLogDef.lineno(__file__), "BJDONG_CD =====> ", BJDONG_CD)
                        raise Exception("BJDONG_CD => " + str(BJDONG_CD))


                    strUniqueKey = strTradeYYYY + "_" +\
                                   sido_code +\
                                   sigu_code+ "_" +\
                                   BJDONG_CD + "_" + \
                                   BONBEON + "_" + \
                                   BUBEON + "_" + \
                                   DEAL_YMD+ "_" +DEPOSIT_AMT+ "_" +RENT_AMT

                    print(GetLogDef.lineno(__file__), "strUniqueKey" , strUniqueKey)

                    sqlSelectMOLIT = "SELECT * FROM "+ConstRealEstateTable_GOV.MolitHouseRealRentMasterTable+" WHERE unique_key = %s "
                    cursorRealEstate.execute(sqlSelectMOLIT , (strUniqueKey) )
                    intMolitCount = cursorRealEstate.rowcount
                    print(GetLogDef.lineno(__file__), "intMolitCount", intMolitCount)

                    if intMolitCount < 1:

                        strDOROJUSO = SIDO_NM + " "
                        strDOROJUSO += SGG_NM + " "
                        strDOROJUSO += BJDONG_NM + " "
                        strDOROJUSO += str(BONBEON).lstrip("0")
                        if len(str(BUBEON).lstrip("0")) > 0:
                            strDOROJUSO += "-"+str(BUBEON).lstrip("0")

                        strNaverLongitude = str(0)
                        strNaverLatitude = str(0)


                        #INSERT
                        sqlInsertMOLIT  = " INSERT INTO "+ConstRealEstateTable_GOV.MolitHouseRealRentMasterTable+" SET "
                        sqlInsertMOLIT += " unique_key = %s"
                        sqlInsertMOLIT += " , SIDO_CD = '"+sido_code+"'"
                        sqlInsertMOLIT += " , SIDO_NM = '"+SIDO_NM+"'"
                        sqlInsertMOLIT += " , SGG_CD = '"+sigu_code+"'"
                        sqlInsertMOLIT += " , SGG_NM = '"+SGG_NM+"'"
                        sqlInsertMOLIT += " , BJDONG_CD = '"+BJDONG_CD+"'"
                        sqlInsertMOLIT += " , BJDONG_NM = '"+BJDONG_NM+"'"
                        sqlInsertMOLIT += " , BONBEON = '"+BONBEON+"'"
                        sqlInsertMOLIT += " , BUBEON = '"+BUBEON+"'"
                        sqlInsertMOLIT += " , BLDG_NM = '"+BLDG_NM+"'"
                        sqlInsertMOLIT += " , BLDG_DONG = '"+BLDG_DONG+"'"
                        sqlInsertMOLIT += " , HOUSE_TYPE = '"+HOUSE_TYPE+"'"
                        sqlInsertMOLIT += " , DEAL_YMD = '"+DEAL_YMD+"'"
                        sqlInsertMOLIT += " , DEPOSIT_AMT = '"+DEPOSIT_AMT+"'"
                        sqlInsertMOLIT += " , RENT_AMT = '" + RENT_AMT + "'"
                        sqlInsertMOLIT += " , PREVIOUS_DEPOSIT_AMT = '" + PREVIOUS_DEPOSIT_AMT + "'"
                        sqlInsertMOLIT += " , PREVIOUS_RENT_AMT = '" + PREVIOUS_RENT_AMT + "'"
                        sqlInsertMOLIT += " , TOT_AREA = '"+TOT_AREA+"'"
                        sqlInsertMOLIT += " , FLOOR = '"+FLOOR+"'"
                        sqlInsertMOLIT += " , BUILD_YEAR = '"+BUILD_YEAR+"'"
                        sqlInsertMOLIT += " , REQ_GBN = '"+REQ_GBN+"'"
                        sqlInsertMOLIT += " , RENEWAL_CONTRACT = '" + RENEWAL_CONTRACT + "'"
                        sqlInsertMOLIT += " , CONTRACT_START = '" + CONTRACT_START + "'"
                        sqlInsertMOLIT += " , CONTRACT_END = '" + CONTRACT_END + "'"
                        sqlInsertMOLIT += " , lng= '" + strNaverLongitude + "' "
                        sqlInsertMOLIT += " , lat= '" + strNaverLatitude + "' "
                        sqlInsertMOLIT += " , geo_point = ST_GeomFromText('POINT(" + strNaverLongitude + " " + strNaverLatitude + ")', 4326,'axis-order=long-lat') "
                        sqlInsertMOLIT += " , AGENT_ADDR = '"+AGENT_ADDR+"'"
                        sqlInsertMOLIT += " , ADDRESS_CODE = '" + sido_code + sigu_code + BJDONG_CD + "'"
                        print(GetLogDef.lineno(__file__), "sqlInsertMOLIT ", sqlInsertMOLIT)

                        cursorRealEstate.execute(sqlInsertMOLIT , (strUniqueKey) )
                        ResRealEstateConnection.commit()
                        nInsertedCount = nInsertedCount + 1

                    print(GetLogDef.lineno(__file__), "END strUniqueKey > ",strUniqueKey)
                    print(GetLogDef.lineno(__file__), "nInsertedCount ", nInsertedCount)
                    print(GetLogDef.lineno(__file__), "nUpdateCount ", nUpdateCount)

                    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                    dictSwitchData = dict()
                    dictSwitchData['result'] = '10'
                    dictSwitchData['data_1'] = dtProcessDay
                    dictSwitchData['data_2'] = strAdminSection
                    dictSwitchData['data_3'] = strGOVMoltyAddressSequence
                    dictSwitchData['data_4'] = nLoop
                    dictSwitchData['data_5'] = nUpdateCount
                    dictSwitchData['data_6'] = nInsertedCount
                    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

                print(GetLogDef.lineno(__file__), "[strAdminName]================== ", strAdminName)
                print(GetLogDef.lineno(__file__), "[dtProcessDay]================== ", dtProcessDay)
                print(GetLogDef.lineno(__file__), "[END LOOP]]================== ", nLoop)

            print(GetLogDef.lineno(__file__), "[END intRangeStart]]================== ", intRangeStart)
            intRangeStart = 0
            print(GetLogDef.lineno(__file__), "[END strAdminSection]]================== ", strAdminSection)
            print(GetLogDef.lineno(__file__), "[END strAdminName]]================== ", strAdminName)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = dtProcessDay
        dictSwitchData['data_2'] = strAdminSection
        dictSwitchData['data_3'] = strGOVMoltyAddressSequence
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.lineno(__file__), "[END strAdminName]]================== ", strAdminName)


    except Exception as e:

        print(GetLogDef.lineno(__file__), "Error Exception")
        print(GetLogDef.lineno(__file__), e)
        print(GetLogDef.lineno(__file__), type(e))
        err_msg = traceback.format_exc()
        print(GetLogDef.lineno(__file__), err_msg)
        print(GetLogDef.lineno(__file__), "strGOVMoltyAddressSequence >> ", strGOVMoltyAddressSequence)


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.lineno(__file__), "[END strAdminName]]================== ", strAdminName)

    else:
        print(GetLogDef.lineno(__file__), 'Inserted => ', nInsertedCount, ' , Updated => ', nUpdateCount)
        print(GetLogDef.lineno(__file__), "========================= SUCCESS END")
    finally:
        print(GetLogDef.lineno(__file__), "Finally END")



if __name__ == '__main__':
    main()