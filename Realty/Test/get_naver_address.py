import sys
sys.path.append("D:/PythonProjects/airstock")

# https://www.data.go.kr/data/15057511/openapi.do
#국토부 아파트 실거래
#SERVICE URL https://www.data.go.kr/data/15057511/openapi.do
#[국토부 아파트 실거래 상세 자료]API URL http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev
#[국토부 아파트 실거래 자료]http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade
import urllib.request
import json
import pymysql
import datetime
import re
import time
import pandas as pd
import requests

from Realty.Government.Init import init_conf
from Lib.RDB import pyMysqlConnector
from dateutil.relativedelta import relativedelta

from Init.Functions.Logs import GetLogDef

from Realty.Government.Const import ConstRealEstateTable_GOV

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
from bs4 import BeautifulSoup
import xml
import xml.etree.ElementTree as ET
try:

    #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
    #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
    #거래 신고 30일 + 취소 신고 +30일
    stToday = DateTime.today()

    nInsertedCount = 0
    nUpdateCount = 0

    #서울 부동산 실거래가 데이터 - 매매
    strProcessType = '034000'
    KuIndex = '00'
    arrCityPlace = '00'
    targetRow = '00'

    nProcessCount=0
    HOUSE_TYPE = '아파트'

    for intSidoLoop in range(2, 5):
        print(GetLogDef.lineno(__file__), "intSidoLoop >> ", intSidoLoop)

    quit(300)



    # # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

    qrySelectSeoulTradeMaster  = "SELECT * FROM " + ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable
    qrySelectSeoulTradeMaster += " WHERE state='00' AND dongmyun_code='00000' AND sigu_code!='000'"
    qrySelectSeoulTradeMaster += " AND seq=4418 "
    qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
    # qrySelectSeoulTradeMaster += " LIMIT 1 "
    cursorRealEstate.execute(qrySelectSeoulTradeMaster)
    row_result = cursorRealEstate.rowcount
    # 등록되어 있는 물건이면 패스

    print(GetLogDef.lineno(__file__), "row_result >> " , row_result)
    rstSelectDatas = cursorRealEstate.fetchall()

    nInsertedCount = 0
    for rstSelectData in rstSelectDatas:

        strGOVMoltyAddressSequence = str(rstSelectData.get('seq'))
        print(GetLogDef.lineno(__file__), "strGOVMoltyAddressSequence >> ", strGOVMoltyAddressSequence)

        sido_code = str(rstSelectData.get('sido_code')).zfill(2)
        sigu_code = rstSelectData.get('sigu_code').zfill(3)

        sido_name = str(rstSelectData.get('sido_name'))
        sigu_name = rstSelectData.get('sigu_name')

        strAdminSection =  sido_code+sigu_code
        strAdminName = sido_name + " " + sigu_name

        print(GetLogDef.lineno(__file__), "strAdminName >> ", strAdminName)
        print(GetLogDef.lineno(__file__), "strAdminSection >> ", strAdminSection)

        dtToday = DateTime.now()

        strDOROJUSO = '세종특별자치시 반곡동 899 수루배마을5단지'


        resultsDict = GeoDataModule.getJusoData(strDOROJUSO)

        # resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)

        print(GetLogDef.lineno(__file__), "resultsDict", resultsDict)
        quit("500")



        for nLoop in range(0, 12):
            # for nLoop in range(0, 730):

            print(GetLogDef.lineno(__file__), "[START LOOP]]================== ", nLoop)

            nbaseDate = dtToday - relativedelta(months=nLoop)
            dtProcessDay = int(nbaseDate.strftime("%Y%m"))

            print(GetLogDef.lineno(__file__), "dtProcessDay >> ", dtProcessDay)

            url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade'

            # url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?_wadl&type=xml'

            params = {'serviceKey': init_conf.MolitDecodedAuthorizationKey, 'LAWD_CD': strAdminSection,'DEAL_YMD': str(dtProcessDay)}

            # url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
            # params = {'serviceKey':  init_conf.MolitDecodedAuthorizationKey , 'LAWD_CD': strAdminSection, 'DEAL_YMD': strDealYMD, 'pageNo': '2', 'numOfRows': '1000'}

            response = requests.get(url, params=params)

            print("response===> ", type(response),  response )

            responseContents = response.text  # page_source 얻기

            print("responseContents===> ", type(responseContents), responseContents)

            ElementResponseRoot = ET.fromstring(responseContents)
            # print("ElementResponseRoot===> ", type(ElementResponseRoot),  ElementResponseRoot, )

            strHeaderResultCode = ElementResponseRoot.find('header').find('resultCode').text
            if strHeaderResultCode != '00':
                print("url===> ", type(url), url)
                print("params===> ", type(params), params)
                raise Exception("strHeaderResultCode => ", strHeaderResultCode)

            objectBodyItemAll = ElementResponseRoot.find('body').find('items')
            print(GetLogDef.lineno(__file__), "objectBodyItemAllCount >> ", len(objectBodyItemAll))
            # intGetCount = len(objectBodyItemAll)
            for objectBodyItem in objectBodyItemAll:
                print(GetLogDef.lineno(__file__), "================objectBodyItem >> ", len(objectBodyItem) , objectBodyItem.tag)
                print(GetLogDef.lineno(__file__), "================Text >> ", type( objectBodyItemAll.iter()) ,  objectBodyItemAll.iter())

                for aaa in objectBodyItem.iter():
                    print(GetLogDef.lineno(__file__), "aaa >> ", aaa.tag ," =>" , aaa.text)

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

                if objectBodyItem.find('거래금액') != None:
                    OBJ_AMT = str(objectBodyItem.find('거래금액').text).strip().replace(",", "")
                if objectBodyItem.find('거래유형') != None:
                    REQ_GBN = str(objectBodyItem.find('거래유형').text).strip()
                if objectBodyItem.find('건축년도') != None:
                    BUILD_YEAR = str(objectBodyItem.find('건축년도').text).strip()
                if objectBodyItem.find('년') != None:
                    strTradeYYYY = str(objectBodyItem.find('년').text).strip().zfill(4)
                if objectBodyItem.find('월') != None:
                    strTradeMM = str(objectBodyItem.find('월').text).strip().zfill(2)
                if objectBodyItem.find('일') != None:
                    strTradeDD = str(objectBodyItem.find('일').text).strip().zfill(2)
                if objectBodyItem.find('동') != None:
                    BLDG_DONG = str(objectBodyItem.find('동').text).strip()
                if objectBodyItem.find('등기일자') != None:
                    REGISTER_YMD = str(objectBodyItem.find('등기일자').text).strip()
                if objectBodyItem.find('매도자') != None:
                    SELLER = str(objectBodyItem.find('매도자').text).strip()
                if objectBodyItem.find('매수자') != None:
                    BUYER = str(objectBodyItem.find('매수자').text).strip()
                if objectBodyItem.find('법정동') != None:
                    BJDONG_NM = str(objectBodyItem.find('법정동').text).strip()
                if objectBodyItem.find('아파트') != None:
                    BLDG_NM = str(objectBodyItem.find('아파트').text).strip().replace('\'',"")
                if objectBodyItem.find('전용면적') != None:
                    TOT_AREA = str(objectBodyItem.find('전용면적').text).strip()
                if objectBodyItem.find('중개사소재지') != None:
                    AGENT_ADDR = str(objectBodyItem.find('중개사소재지').text).strip()
                if objectBodyItem.find('지번') != None:
                    BJD_JIUN = str(objectBodyItem.find('지번').text).strip()
                if objectBodyItem.find('지역코드') != None:
                    SGG_CD = str(objectBodyItem.find('지역코드').text).strip()
                if objectBodyItem.find('층') != None:
                    FLOOR = str(objectBodyItem.find('층').text).strip()
                if objectBodyItem.find('해제여부') != None:
                    CANCEL_YN = str(objectBodyItem.find('해제여부').text).strip()
                if objectBodyItem.find('해제사유발생일') != None:
                    CNTL_YMD = str(objectBodyItem.find('해제사유발생일').text).replace(".", "")

                DEAL_YMD = strTradeYYYY + strTradeMM + strTradeDD

                print(GetLogDef.lineno(__file__), "BJD_JIUN =====> ", BJD_JIUN)

                listBJD_JIUN = BJD_JIUN.split("-")
                print(GetLogDef.lineno(__file__), "listBJD_JIUN =====> ", listBJD_JIUN , len(listBJD_JIUN) , type(listBJD_JIUN))

                BONBEON='0000'
                BUBEON = '0000'

                if len(listBJD_JIUN) == 1:
                    BONBEON = re.sub(r'[^0-9]', '', listBJD_JIUN[0]).zfill(4)
                elif len(listBJD_JIUN) == 2:
                    BONBEON = re.sub(r'[^0-9]', '', listBJD_JIUN[0]).zfill(4)
                    BUBEON = re.sub(r'[^0-9]', '', listBJD_JIUN[1]).zfill(4)
                elif len(listBJD_JIUN) > 2:
                    listTemp = []
                    for intTempLoop in range(len(listBJD_JIUN)):
                        strTemp = re.sub(r'[^0-9]', '', listBJD_JIUN[intTempLoop])
                        if len(listTemp) > 1:
                            listTemp.append(strTemp)

                    if len(listTemp) == 1:
                        BONBEON = re.sub(r'[^0-9]', '', listTemp[0]).zfill(4)
                    elif len(listTemp) == 2:
                        BONBEON = re.sub(r'[^0-9]', '', listTemp[0]).zfill(4)
                        BUBEON = re.sub(r'[^0-9]', '', listTemp[1]).zfill(4)

                    elif len(listTemp) > 2:
                        print(GetLogDef.lineno(__file__), "listTemp =====> ", listTemp)
                        raise Exception("listTemp => " + str(listTemp))

                print(GetLogDef.lineno(__file__), "BONBEON =====> ", BONBEON)

                if len(listBJD_JIUN) > 1:
                    BUBEON = listBJD_JIUN[1].zfill(4)

                print(GetLogDef.lineno(__file__), "BUBEON =====> ", BUBEON)

                state = '00'
                CNTL_YMD=''
                if len(CANCEL_YN) > 0:
                    state = '10'
                    CNTL_YMD = "20" + CNTL_YMD



                sqlSelectGOVCodeinfo  = " SELECT * FROM kt_realty_gov_code_info WHERE sido_code=%s AND  sigu_code=%s "
                sqlSelectGOVCodeinfo += " AND dongmyun_name = %s"
                print(GetLogDef.lineno(__file__), "sqlSelectGOVCodeinfo =====> ", sqlSelectGOVCodeinfo ,sido_code , sigu_code ,BJDONG_NM )
                cursorRealEstate.execute(sqlSelectGOVCodeinfo , (sido_code , sigu_code ,BJDONG_NM ) )
                intGovCodeCount = cursorRealEstate.rowcount

                if intGovCodeCount != 1:
                    print(GetLogDef.lineno(__file__), "intGovCodeCount =====> ", intGovCodeCount)
                    raise Exception("intGovCodeCount => " + str(intGovCodeCount))

                rstSelectDatas = cursorRealEstate.fetchone()
                BJDONG_CD = rstSelectDatas.get('dongmyun_code')
                BJDONG_NM = rstSelectDatas.get('dongmyun_name')
                SIDO_NM = rstSelectDatas.get('sido_name')
                SGG_NM = rstSelectDatas.get('sigu_name')

                strUniqueKey = strTradeYYYY + "_" +\
                               SGG_CD + "_" +\
                               BJDONG_CD+ "_" +\
                               BJDONG_CD + "_" + \
                               BONBEON + "_" + \
                               BUBEON + "_" + \
                               FLOOR+ "_" +\
                               DEAL_YMD+ "_" +OBJ_AMT

                print(GetLogDef.lineno(__file__), "strUniqueKey" , strUniqueKey)
                sqlSelectMOLIT = "SELECT * FROM kt_realty_molit_real_trade_master WHERE unique_key = %s "
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

                    resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)

                    print(GetLogDef.lineno(__file__), "resultsDict", resultsDict)


                    nInsertedCount = nInsertedCount + 1
                else:

                    rstSelectMOLIT = cursorRealEstate.fetchone()
                    DBstate = rstSelectMOLIT.get('state')

                    print(GetLogDef.lineno(__file__), "UPDATE SET ", strUniqueKey)
                    print(GetLogDef.lineno(__file__), DBstate, type(DBstate), " != ", state, type(state))

                    if DBstate != '00':
                        print(GetLogDef.lineno(__file__), "-----------------------------------------")
                        continue

                    print(GetLogDef.lineno(__file__), "-----------------------------------------")

                    nUpdateCount = nUpdateCount + 1

                print(GetLogDef.lineno(__file__), "nInsertedCount ", nInsertedCount)
                print(GetLogDef.lineno(__file__), "nUpdateCount ", nUpdateCount)

            print(GetLogDef.lineno(__file__), "[END LOOP]]================== ", nLoop)

            print(GetLogDef.lineno(__file__), "========================================time.sleep(1) ", strUniqueKey)
            time.sleep(2)
        print(GetLogDef.lineno(__file__), "[END strAdminSection]]================== ", strAdminSection)
        print(GetLogDef.lineno(__file__), "strGOVMoltyAddressSequence >> ", strGOVMoltyAddressSequence)









                #
                #
                #
                # else:
                #
                #     # sqlUpdateMOLIT  = " UPDATE kt_realty_molit_real_trade_master SET "
                #     # sqlUpdateMOLIT += " WHERE unique_key = %s"






    # aaa = ElementResponseRoot.parse('item')

    # ElementResponseRootBody = ElementResponseRoot.findall('body')




    # vvvvs = ElementResponseRoot.find('item')

    # print("vvvv =>=============== ", type(objectBodyItems), objectBodyItems)





    # for objectBodyItem in objectBodyItemAll:
    #
    #     # print(objectBodyItem.find('거래금액').text)
    #     OBJ_AMT =str(objectBodyItem.find('거래금액').text).strip().replace(",","")
    #     REQ_GBN = str(objectBodyItem.find('거래유형').text).strip()
    #     BUILD_YEAR = str(objectBodyItem.find('건축년도').text).strip()
    #     strTradeYYYY = str(objectBodyItem.find('년').text).strip().zfill(4)
    #     strTradeMM = str(objectBodyItem.find('월').text).strip().zfill(2)
    #     strTradeDD = str(objectBodyItem.find('일').text).strip().zfill(2)
    #     CNTL_YMD =strTradeYYYY + strTradeMM + strTradeDD
    #     BLDG_DONG = str(objectBodyItem.find('동').text).strip()
    #     REGISTER_YMD = str(objectBodyItem.find('등기일자').text).strip()
    #
    #     SELLER = str(objectBodyItem.find('매도자').text).strip()
    #     BUYER = str(objectBodyItem.find('매수자').text).strip()
    #
    #     BJDONG_NM = str(objectBodyItem.find('법정동').text).strip()
    #     BLDG_NM =  str(objectBodyItem.find('아파트').text).strip()
    #     TOT_AREA = str(objectBodyItem.find('전용면적').text).strip()
    #
    #     AGENT_ADDR = str(objectBodyItem.find('중개사소재지').text).strip()
    #
    #     BJD_JIUN = str(objectBodyItem.find('지번').text).strip()
    #     listBJD_JIUN = BJD_JIUN.strip("-")
    #     BONBEON = listBJD_JIUN[0].zfill(4)
    #     if len(listBJD_JIUN) > 1:
    #         BUBEON = listBJD_JIUN[1].zfill(4)
    #
    #     SGG_CD = str(objectBodyItem.find('지역코드').text).strip()
    #     FLOOR = str(objectBodyItem.find('층').text).strip()
    #     CNTL_YMD = "20"+str(objectBodyItem.find('해제사유발생일').text).replace(".","")
    #     CANCEL_YN = str(objectBodyItem.find('해제여부').text).strip()
    #     state = '00'
    #     if len(CANCEL_YN) > 0:
    #         state = '10'


        # 만리동2가


        # print("===================================================================================== ")
    #
    # objectBodyItem = objectBodyItems.findall('item')
    # print(GetLogDef.lineno(__file__), "cccc =>=============== ", type(objectBodyItems), objectBodyItems)
    # nLoopCount = 0
    # for objectItem in objectBodyItem:
    #     print(nLoopCount,"===================================================================================== ")
    #     for objectItemTag in objectItem.iter():
    #         strItemTagName = objectItemTag.tag
    #         strItemTagValue = objectItemTag.text
    #
    #         print(GetLogDef.lineno(__file__), "objectItemTag =>=============== [", objectItemTag.tag, "]" , objectItemTag.text)
    #
    #     nLoopCount += 1
    #
    #


    # # print("ElementResponseRootBody => ",  ElementResponseRootBody)
    #
    # ElementResponseRootBodyItems = ElementResponseRoot.findall('item')
    #
    # print("ElementResponseRootBodyItems => ",  ElementResponseRootBodyItems)
    #
    # print("ElementResponseRootBodyItems => ", len(ElementResponseRootBodyItems))

    #
    # for ElementResponseRootBodyItem in ElementResponseRootBodyItems:
    #     # print("ElementResponseRootBodyItem => ", type(ElementResponseRootBodyItem), ElementResponseRootBodyItem.text)
    #     print("ElementResponseRootBodyItem => ", ElementResponseRootBodyItem.text)
    #



    # responseXml = BeautifulSoup(responseContents)


    #
    #
    #
    # ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
    #
    # cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
    #
    # sqlSelectGovCode  = " SELECT * FROM "+ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable + " WHERE state='00' "
    # sqlSelectGovCode += " LIMIT 10 "
    #
    #
    # cursorRealEstate.execute(sqlSelectGovCode)
    # intSelectCount = cursorRealEstate.rowcount
    #
    #
    #
    #
    #
    #
    # rstSelectDatas = cursorRealEstate.fetchall()
    # for rstSelectData in rstSelectDatas:
    #     nProcessCount += 1
    #
    #     print(GetLogDef.lineno(__file__), "rstSelectData >", rstSelectData)
    #
    #     strSidoCode = str(rstSelectData.get('sido_code'))
    #     strSiGuCode = str(rstSelectData.get('sigu_code'))
    #
    #
    #
    #
    #     #
    #     # print(GetLogDef.lineno(__file__), "nStartNumber >", nStartNumber)
    #     # print(GetLogDef.lineno(__file__), "nEndNumber >", nEndNumber)
    #     #
    #     # # # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
    #     # # url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/tbLnOpendataRtmsV/"+str(nStartNumber)+"/"+str(nEndNumber)+"/"+strProcessYear
    #     #
    #     # # 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
    #
    # # url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
    # # params = {'serviceKey':  init_conf.MolitDecodedAuthorizationKey , 'pageNo': '1', 'numOfRows': '1000', 'LAWD_CD': '11110', 'DEAL_YMD': '201512'}
    # #
    # # response = requests.get(url, params=params)
    # # print(response.content)
    # #
    # #






except Exception as e:

    print(GetLogDef.lineno(__file__), "Error Exception")
    print(GetLogDef.lineno(__file__), e)
    print(GetLogDef.lineno(__file__), type(e))
    print(GetLogDef.lineno(__file__), "strGOVMoltyAddressSequence >> ", strGOVMoltyAddressSequence)


    # # Switch 오류 업데이트
    # dictSeoulSwitch = {}
    # dictSeoulSwitch['seq'] = dtProcessDay
    # dictSeoulSwitch['state'] = 20
    # print(GetLogDef.lineno(__file__), "dtProcessDay >", type(dtProcessDay), dictSeoulSwitch)
    # print(GetLogDef.lineno(__file__), "dictSeoulSwitch >", type(dtProcessDay), dictSeoulSwitch)
    # bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulUpdate(dictSeoulSwitch)
    # print(GetLogDef.lineno(__file__), "bSwitchUpdateResult >", bSwitchUpdateResult)
    #
    # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    # dictSwitchData = dict()
    # dictSwitchData['result'] = '30'
    # if dtProcessDay is not None:
    #     dictSwitchData['data_1'] = dtProcessDay
    #
    # if strProcessDay is not None:
    #     dictSwitchData['data_2'] = strProcessDay
    #
    # if nStartNumber is not None:
    #     dictSwitchData['data_3'] = nStartNumber
    #
    # if nEndNumber is not None:
    #     dictSwitchData['data_3'] = nEndNumber

    # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)




else:
    print(GetLogDef.lineno(__file__), 'Inserted => ', nInsertedCount, ' , Updated => ', nUpdateCount)
    print(GetLogDef.lineno(__file__), "========================= SUCCESS END")
finally:
    print(GetLogDef.lineno(__file__), "Finally END")


