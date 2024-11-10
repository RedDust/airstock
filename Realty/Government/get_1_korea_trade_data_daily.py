import sys
sys.path.append("D:/PythonProjects/airstock")

# https://www.data.go.kr/data/15057511/openapi.do
#-> https://www.data.go.kr/data/15126469/openapi.do

#국토교통부 실거래가 정보 - 아파트 매매 실거래가 자료
#SERVICE URL https://www.data.go.kr/data/15126469/openapi.do
#[국토부 아파트 실거래 상세 자료]API URL http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev
#[국토부 아파트 실거래 자료]API URL 	https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade
#
#ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable = 'kt_realty_gov_code_info'
#ConstRealEstateTable_GOV.MolitRealTradeMasterTable = 'kt_realty_molit_real_trade_master'
#ConstRealEstateTable_GOV.MolitRealTradeCancelTable = 'kt_realty_molit_real_trade_master_cancel'



import urllib.request
import json
import pymysql
import traceback
import time
import logging
import logging.handlers
import inspect
import traceback
import re
import pandas as pd
import requests
from pandas.io.json import json_normalize
from Realty.Government.Init import init_conf
from Lib.RDB import pyMysqlConnector
from dateutil.relativedelta import relativedelta

from Init.Functions.Logs import GetLogDef
from Realty.Government.Const import ConstRealEstateTable_GOV
from Init.DefConstant import ConstRealEstateTable


from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
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
        strProcessType = '035111'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'

        nProcessCount=0
        HOUSE_TYPE = '아파트'

        # # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        strSwitchSidoCode=''
        strSwitchYYYYMM=''
        GOVMoltyAddressSequence='0'
        intLoopStart=0
        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            QuitException.QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno)+ 'strResult => '+ strResult)  # 예외를 발생시킴

        if strResult == '10':
            QuitException.QuitException(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno)+ 'It is currently in operation. => '+ strResult)  # 예외를 발생시킴

        if strResult == '20':
            intLoopStart = str(rstResult.get('data_4'))
            GOVMoltyAddressSequence = str(rstResult.get('data_3'))
            strSwitchSidoCode = str(rstResult.get('data_2'))
            strSwitchYYYYMM = str(rstResult.get('data_1'))

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_5'] = nUpdateCount
        dictSwitchData['data_6'] = nInsertedCount
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)



        qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
        qrySelectSeoulTradeMaster += " WHERE state='00' AND sgg_cd<>'000' AND umd_cd='000' AND ri_cd='00'"
        qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
        # qrySelectSeoulTradeMaster += " LIMIT 1 "

        # qrySelectSeoulTradeMaster  = "SELECT * FROM " + ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable
        # qrySelectSeoulTradeMaster += " WHERE state='00' AND dongmyun_code='00000' AND sigu_code!='000'"
        # qrySelectSeoulTradeMaster += " AND seq >= "+GOVMoltyAddressSequence+" "
        # qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
        # # qrySelectSeoulTradeMaster += " LIMIT 100 "
        cursorRealEstate.execute(qrySelectSeoulTradeMaster)
        row_result = cursorRealEstate.rowcount
        # 등록되어 있는 물건이면 패스


        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "row_result >> " , row_result)
        rstSelectDatas = cursorRealEstate.fetchall()

        intRangeStart = int(intLoopStart)
        intRangeEnd = 12 * 25
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

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "[START LOOP]]================== ", nLoop)

                nbaseDate = dtToday - relativedelta(months=nLoop)
                dtProcessDay = str(int(nbaseDate.strftime("%Y%m")))

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "dtProcessDay >> ", dtProcessDay)

                # url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'
                url = 'https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade'

                params = {'serviceKey': init_conf.MolitDecodedAuthorizationKey, 'LAWD_CD': strAdminSection,'DEAL_YMD': str(dtProcessDay)}
                # print("url===> ", strAdminSection, dtProcessDay, url)
                # response = requests.get(url, params=params)
                # print("response===> ", type(response),  response )
                # print("response.status_code===> ", type(response.status_code), response.status_code)
                # print("response.raise_for_status()===> ", type(response.raise_for_status()), response.raise_for_status())

                # if response.status_code != 200:
                while True:
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "============================time.sleep(2) ")
                    time.sleep(1)
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





                objectBodyItemAll = ElementResponseRoot.find('body').find('items')
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "objectBodyItemAllCount >> ", len(objectBodyItemAll))
                # intGetCount = len(objectBodyItemAll)
                for objectBodyItem in objectBodyItemAll:


                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "=============================================================objectBodyItem >> ")


                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "================objectBodyItem >> ", len(objectBodyItem) , objectBodyItem.tag)
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "================Text >> ", type( objectBodyItemAll.iter()) ,  objectBodyItemAll.iter())

                    for aaa in objectBodyItem.iter():
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "objectBodyItem.iter() >> ", aaa.tag ," =>" , aaa.text)

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
                    CANCEL_YN=''
                    CNTL_YMD=''

                    print("objectBodyItem>>" , objectBodyItem)


                    if objectBodyItem.find('dealAmount') != None:
                        OBJ_AMT = str(objectBodyItem.find('dealAmount').text).strip().replace(",", "")
                    if objectBodyItem.find('dealingGbn') != None:
                        REQ_GBN = str(objectBodyItem.find('dealingGbn').text).strip()
                    if objectBodyItem.find('buildYear') != None:
                        BUILD_YEAR = str(objectBodyItem.find('buildYear').text).strip()
                    if objectBodyItem.find('dealYear') != None:
                        strTradeYYYY = str(objectBodyItem.find('dealYear').text).strip().zfill(4)
                    if objectBodyItem.find('dealMonth') != None:
                        strTradeMM = str(objectBodyItem.find('dealMonth').text).strip().zfill(2)
                    if objectBodyItem.find('dealDay') != None:
                        strTradeDD = str(objectBodyItem.find('dealDay').text).strip().zfill(2)
                    if objectBodyItem.find('aptDong') != None:
                        BLDG_DONG = str(objectBodyItem.find('aptDong').text).strip()
                    if objectBodyItem.find('rgstDate') != None:
                        REGISTER_YMD = str(objectBodyItem.find('rgstDate').text).strip()
                    if objectBodyItem.find('slerGbn') != None:
                        SELLER = str(objectBodyItem.find('slerGbn').text).strip()
                    if objectBodyItem.find('buyerGbn') != None:
                        BUYER = str(objectBodyItem.find('buyerGbn').text).strip()
                    if objectBodyItem.find('umdNm') != None:
                        BJDONG_NM = str(objectBodyItem.find('umdNm').text).strip()
                    if objectBodyItem.find('aptNm') != None:
                        BLDG_NM = str(objectBodyItem.find('aptNm').text).strip().replace('\'',"")
                    if objectBodyItem.find('excluUseAr') != None:
                        TOT_AREA = str(objectBodyItem.find('excluUseAr').text).strip()
                    if objectBodyItem.find('estateAgentSggNm') != None:
                        AGENT_ADDR = str(objectBodyItem.find('estateAgentSggNm').text).strip()
                    if objectBodyItem.find('jibun') != None:
                        BJD_JIUN = str(objectBodyItem.find('jibun').text).strip()
                    if objectBodyItem.find('sggCd') != None:
                        SGG_CD = str(objectBodyItem.find('sggCd').text).strip()
                    if objectBodyItem.find('floor') != None:
                        FLOOR = str(objectBodyItem.find('floor').text).strip()
                    if objectBodyItem.find('cdealType') != None:
                        CANCEL_YN = str(objectBodyItem.find('cdealType').text).strip()
                    if objectBodyItem.find('cdealDay') != None:
                        CNTL_YMD = str(objectBodyItem.find('cdealDay').text).replace(".", "")


                    DEAL_YMD = strTradeYYYY + strTradeMM + strTradeDD

                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "DEAL_YMD =====> ", DEAL_YMD)

                    listBJD_JIUN = BJD_JIUN.split("-")
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "listBJD_JIUN =====> ", listBJD_JIUN , len(listBJD_JIUN) , type(listBJD_JIUN))

                    if len(listBJD_JIUN) == 1:
                        BONBEON = re.sub(r'[^0-9]', '', listBJD_JIUN[0]).zfill(4)
                        BUBEON = '0000'
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "BONBEON=====> ", BONBEON, len(BONBEON),type(BONBEON))

                    elif len(listBJD_JIUN) == 2:
                        BONBEON = re.sub(r'[^0-9]', '', listBJD_JIUN[0]).zfill(4)
                        BUBEON = re.sub(r'[^0-9]', '', listBJD_JIUN[1]).zfill(4)
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "BONBEON=====> ", BONBEON, len(BONBEON), type(BONBEON))

                    elif len(listBJD_JIUN) > 2:
                        listTemp = []
                        for intTempLoop in range(len(listBJD_JIUN)):
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "intTempLoop=====> ", intTempLoop, type(intTempLoop))
                            strTemp = re.sub(r'[^0-9]', '', listBJD_JIUN[intTempLoop])

                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "strTemp=====> ", strTemp, type(strTemp))

                            if len(strTemp) > 0:
                                listTemp.append(strTemp)

                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "listTemp=====> ", listTemp, len(listTemp), type(listTemp))

                        if len(listTemp) == 1:
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "BONBEON=====> ", BONBEON, len(BONBEON), type(BONBEON))
                            BONBEON = re.sub(r'[^0-9]', '', listTemp[0]).zfill(4)
                            BUBEON = '0000'
                        elif len(listTemp) == 2:
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "listTemp=====> ", listTemp, len(listTemp), type(listTemp))
                            BONBEON = re.sub(r'[^0-9]', '', listTemp[0]).zfill(4)
                            BUBEON = re.sub(r'[^0-9]', '', listTemp[1]).zfill(4)

                        elif len(listTemp) > 2:
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "listTemp =====> ", listTemp)
                            BONBEON = re.sub(r'[^0-9]', '', listTemp[0]).zfill(4)
                            BUBEON = re.sub(r'[^0-9]', '', listTemp[1]).zfill(4)

                    else:
                        raise Exception("listBJD_JIUN => " + str(listBJD_JIUN))

                    state = '00'
                    if len(CANCEL_YN) > 0:
                        state = '10'
                        CNTL_YMD = "20" + CNTL_YMD


                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno),"BJDONG_NM" , "["+BJDONG_NM+"]")

                    sqlSelectGOVCodeinfo  = " SELECT * FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
                    sqlSelectGOVCodeinfo += " WHERE sido_cd='"+sido_code+"' AND sgg_cd='"+sigu_code+"' "
                    sqlSelectGOVCodeinfo += " AND locatadd_nm LIKE '% " + BJDONG_NM + "' "
                    print(GetLogDef.lineno(__file__), "sqlSelectGOVCodeinfo =====> ", sqlSelectGOVCodeinfo ,sido_code , sigu_code )
                    cursorRealEstate.execute(sqlSelectGOVCodeinfo)
                    intGovCodeCount = cursorRealEstate.rowcount

                    print(GetLogDef.lineno(__file__), "intGovCodeCount =====> ", intGovCodeCount)

                    if intGovCodeCount != 1:
                        print(GetLogDef.lineno(__file__), "sqlSelectGOVCodeinfo =====> ", sqlSelectGOVCodeinfo)
                        raise Exception("intGovCodeCount => " + str(intGovCodeCount))
                    else:
                        rstSelectDatas = cursorRealEstate.fetchone()
                        BJDONG_CD = rstSelectDatas.get('umd_cd') + rstSelectDatas.get('ri_cd')
                        BJDONG_NM = rstSelectDatas.get('umd_nm') + " " + rstSelectDatas.get('ri_nm')
                        SIDO_NM = rstSelectDatas.get('sido_nm')
                        SGG_NM = rstSelectDatas.get('sgg_nm')


                    if len(BJDONG_CD) < 5:
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "BJDONG_CD =====> ", BJDONG_CD)
                        raise Exception("BJDONG_CD => " + str(BJDONG_CD))


                    strUniqueKey = strTradeYYYY + "_" +\
                                   sido_code + "_" +\
                                   sigu_code+ "_" +\
                                   BJDONG_CD + "_" + \
                                   BONBEON + "_" + \
                                   BUBEON + "_" + \
                                   FLOOR+ "_" +\
                                   DEAL_YMD+ "_" +OBJ_AMT

                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "strUniqueKey" , strUniqueKey)


                    sqlSelectMOLIT = "SELECT * FROM "+ConstRealEstateTable_GOV.MolitRealTradeMasterTable+" WHERE unique_key = %s "
                    cursorRealEstate.execute(sqlSelectMOLIT , (strUniqueKey) )
                    intMolitCount = cursorRealEstate.rowcount
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "intMolitCount", intMolitCount)

                    if intMolitCount < 1:

                        strNaverLongitude = str(0)
                        strNaverLatitude = str(0)

                        # strDOROJUSO = SIDO_NM + " "
                        # strDOROJUSO += SGG_NM + " "
                        # strDOROJUSO += BJDONG_NM + " "
                        # strDOROJUSO += str(BONBEON).lstrip("0")
                        # if len(str(BUBEON).lstrip("0")) > 0:
                        #     strDOROJUSO += "-"+str(BUBEON).lstrip("0")
                        #
                        # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                        #                         inspect.getframeinfo(inspect.currentframe()).lineno), "strDOROJUSO",
                        #       strDOROJUSO)
                        #
                        # resultsDict = GeoDataModule.getJusoData(strDOROJUSO)
                        # print(GetLogDef.lineno(__file__), "resultsDict >", type(resultsDict),
                        #       isinstance(resultsDict, dict),
                        #       resultsDict)
                        # if isinstance(resultsDict, dict) == True:
                        #     print(GetLogDef.lineno(__file__), resultsDict['jibunAddr'])
                        #     strDOROJUSO = str(resultsDict['roadAddrPart1']).strip()
                        #
                        # resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
                        # print(GetLogDef.lineno(__file__), "resultsDict >", type(resultsDict), resultsDict)
                        # print(GetLogDef.lineno(__file__), "resultsDict >", type(resultsDict), isinstance(resultsDict, dict), resultsDict)
                        #
                        # if isinstance(resultsDict, dict) != False:
                        #     strNaverLongitude = str(resultsDict['x'])  # 127
                        #     strNaverLatitude = str(resultsDict['y'])  # 37

                        #INSERT
                        sqlInsertMOLIT  = " INSERT INTO "+ConstRealEstateTable_GOV.MolitRealTradeMasterTable+" SET "
                        sqlInsertMOLIT += " unique_key = %s"
                        sqlInsertMOLIT += " , ACC_YEAR = '"+strTradeYYYY+"'"
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
                        sqlInsertMOLIT += " , OBJ_AMT = '"+OBJ_AMT+"'"
                        sqlInsertMOLIT += " , TOT_AREA = '"+TOT_AREA+"'"
                        sqlInsertMOLIT += " , FLOOR = '"+FLOOR+"'"
                        sqlInsertMOLIT += " , CNTL_YMD = '"+CNTL_YMD+"'"
                        sqlInsertMOLIT += " , REGISTER_YMD = '"+REGISTER_YMD+"'"
                        sqlInsertMOLIT += " , BUILD_YEAR = '"+BUILD_YEAR+"'"
                        sqlInsertMOLIT += " , SELLER = '" + SELLER + "'"
                        sqlInsertMOLIT += " , BUYER = '" + BUYER + "'"
                        sqlInsertMOLIT += " , REQ_GBN = '"+REQ_GBN+"'"
                        sqlInsertMOLIT += " , lng= '" + strNaverLongitude + "' "
                        sqlInsertMOLIT += " , lat= '" + strNaverLatitude + "' "
                        sqlInsertMOLIT += " , geo_point = ST_GeomFromText('POINT(" + strNaverLongitude + " " + strNaverLatitude + ")', 4326,'axis-order=long-lat') "
                        sqlInsertMOLIT += " , AGENT_ADDR = '"+AGENT_ADDR+"'"
                        sqlInsertMOLIT += " , ADDRESS_CODE = '" + sido_code + sigu_code + BJDONG_CD + "'"
                        sqlInsertMOLIT += " , state = '"+state+"'"

                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "sqlInsertMOLIT ", sqlInsertMOLIT)

                        cursorRealEstate.execute(sqlInsertMOLIT , (strUniqueKey) )
                        ResRealEstateConnection.commit()
                        nInsertedCount = nInsertedCount + 1

                    else:

                        rstSelectMOLIT = cursorRealEstate.fetchone()
                        DBstate = rstSelectMOLIT.get('state')
                        DBREGISTER_YMD = rstSelectMOLIT.get('REGISTER_YMD')


                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "UPDATE SET ", strUniqueKey)
                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), DBstate, type(DBstate), " != ", state, type(state))

                        # DB는 정상이고, 조회는 정상이 아닌경우 취소 처리
                        if DBstate == '00' and DBstate != state:

                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "state=>" , state)
                            sqlSelectMOLITCancel = "SELECT * FROM "+ConstRealEstateTable_GOV.MolitRealTradeCancelTable+" WHERE unique_key = %s "
                            cursorRealEstate.execute(sqlSelectMOLITCancel, (strUniqueKey))
                            intMolitCancelCount = cursorRealEstate.rowcount
                            if intMolitCancelCount < 0:
                                sqlInsertMOLITCancel = " INSERT INTO "+ConstRealEstateTable_GOV.MolitRealTradeCancelTable+" SET "
                                sqlInsertMOLITCancel += " unique_key = %s"
                                sqlInsertMOLITCancel += " , ACC_YEAR = '" + strTradeYYYY + "'"
                                sqlInsertMOLITCancel += " , SIDO_CD = '" + sido_code + "'"
                                sqlInsertMOLITCancel += " , SIDO_NM = '" + SIDO_NM + "'"
                                sqlInsertMOLITCancel += " , SGG_CD = '" + sigu_code + "'"
                                sqlInsertMOLITCancel += " , SGG_NM = '" + SGG_NM + "'"
                                sqlInsertMOLITCancel += " , BJDONG_CD = '" + BJDONG_CD + "'"
                                sqlInsertMOLITCancel += " , BJDONG_NM = '" + BJDONG_NM + "'"
                                sqlInsertMOLITCancel += " , BONBEON = '" + BONBEON + "'"
                                sqlInsertMOLITCancel += " , BUBEON = '" + BUBEON + "'"
                                sqlInsertMOLITCancel += " , BLDG_NM = '" + BLDG_NM + "'"
                                sqlInsertMOLITCancel += " , BLDG_DONG = '" + BLDG_DONG + "'"
                                sqlInsertMOLITCancel += " , HOUSE_TYPE = '" + HOUSE_TYPE + "'"
                                sqlInsertMOLITCancel += " , DEAL_YMD = '" + DEAL_YMD + "'"
                                sqlInsertMOLITCancel += " , OBJ_AMT = '" + OBJ_AMT + "'"
                                sqlInsertMOLITCancel += " , TOT_AREA = '" + TOT_AREA + "'"
                                sqlInsertMOLITCancel += " , FLOOR = '" + FLOOR + "'"
                                sqlInsertMOLITCancel += " , CNTL_YMD = '" + CNTL_YMD + "'"
                                sqlInsertMOLITCancel += " , REGISTER_YMD = '" + REGISTER_YMD + "'"
                                sqlInsertMOLITCancel += " , BUILD_YEAR = '" + BUILD_YEAR + "'"
                                sqlInsertMOLITCancel += " , SELLER = '" + SELLER + "'"
                                sqlInsertMOLITCancel += " , BUYER = '" + BUYER + "'"
                                sqlInsertMOLITCancel += " , REQ_GBN = '" + REQ_GBN + "'"
                                sqlInsertMOLITCancel += " , lng= '0' "
                                sqlInsertMOLITCancel += " , lat= '0' "
                                sqlInsertMOLITCancel += " , geo_point = ST_GeomFromText('POINT( 0 0 )', 4326,'axis-order=long-lat') "
                                sqlInsertMOLITCancel += " , AGENT_ADDR = '" + AGENT_ADDR + "'"
                                sqlInsertMOLITCancel += " , state = '" + state + "'"
                                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "sqlInsertMOLITCancel ", sqlInsertMOLITCancel)
                                cursorRealEstate.execute(sqlInsertMOLITCancel, (strUniqueKey))

                            sqlUpdateMOLIT = " UPDATE " + ConstRealEstateTable_GOV.MolitRealTradeMasterTable + " SET "
                            sqlUpdateMOLIT += " CNTL_YMD = '" + CNTL_YMD + "'"
                            sqlUpdateMOLIT += " , state = '" + state + "'"
                            sqlUpdateMOLIT += " , modify_date = NOW() "
                            sqlUpdateMOLIT += " WHERE unique_key = %s"
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "sqlUpdateMOLIT ", sqlUpdateMOLIT)
                            cursorRealEstate.execute(sqlUpdateMOLIT, (strUniqueKey))
                            ResRealEstateConnection.commit()
                            nUpdateCount = nUpdateCount + 1


                        #DB에 이미 등기 되었거나,  등기 된 내역이 아니면
                        if len(DBREGISTER_YMD) > 1 and len(REGISTER_YMD) < 0:

                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "DBREGISTER_YMD=>" , DBREGISTER_YMD)
                            sqlUpdateMOLIT = " UPDATE " + ConstRealEstateTable_GOV.MolitRealTradeMasterTable + " SET "
                            sqlUpdateMOLIT += " REGISTER_YMD = '" + REGISTER_YMD + "'"
                            sqlUpdateMOLIT += " , modify_date = NOW() "
                            sqlUpdateMOLIT += " WHERE unique_key = %s"
                            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "sqlUpdateMOLIT ", sqlUpdateMOLIT)
                            cursorRealEstate.execute(sqlUpdateMOLIT, (strUniqueKey))
                            ResRealEstateConnection.commit()
                            nUpdateCount = nUpdateCount + 1

                        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "END strUniqueKey > ", strUniqueKey)


                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "END strUniqueKey > ",strUniqueKey)
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "nInsertedCount ", nInsertedCount)
                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "nUpdateCount ", nUpdateCount)

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

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "[strAdminName]================== ", strAdminName)
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "[dtProcessDay]================== ", dtProcessDay)
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "[END LOOP]]================== ", nLoop)

            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "[END intRangeStart]]================== ", intRangeStart)
            intRangeStart = 0
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "[END strAdminSection]]================== ", strAdminSection)
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "[END strAdminName]]================== ", strAdminName)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = dtProcessDay
        dictSwitchData['data_2'] = strAdminSection
        dictSwitchData['data_3'] = strGOVMoltyAddressSequence
        dictSwitchData['data_4'] = nLoop
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "[END strAdminName]]================== ", strAdminName)


    except Exception as e:

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "Error Exception")
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), e)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), type(e))
        err_msg = traceback.format_exc()
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), err_msg)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "strGOVMoltyAddressSequence >> ", strGOVMoltyAddressSequence)


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "[END strAdminName]]================== ", strAdminName)

    else:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), 'Inserted => ', nInsertedCount, ' , Updated => ', nUpdateCount)
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "========================= SUCCESS END")
    finally:
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno), "Finally END")


