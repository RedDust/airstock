import sys
sys.path.append("D:/PythonProjects/airstock")

# https://www.data.go.kr/data/15057267/openapi.do
#국토교통부_상업업무용 부동산 매매 신고 자료
#SERVICE URL https://www.data.go.kr/data/15057267/openapi.do
#[국토교통부_상업업무용 부동산 매매 신고 자료]API http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcNrgTrade

#ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable = 'kt_realty_gov_code_info'
#ConstRealEstateTable_GOV.MolitBusinessRealTradeMasterTable = 'kt_realty_molit_business_real_trade_master'
#ConstRealEstateTable_GOV.MolitBusinessRealTradeCancelTable = 'kt_realty_molit_business_real_trade_master_cancel'

import urllib.request
import pymysql
import traceback
import time
import re
import requests
from Realty.Government.Init import init_conf
from Lib.RDB import pyMysqlConnector
from dateutil.relativedelta import relativedelta

from Init.Functions.Logs import GetLogDef

from Realty.Government.Const import ConstRealEstateTable_GOV

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
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
        strProcessType = '035151'
        strGOVMoltyAddressSequence = '0'
        strAdminName = ''
        targetRow = '00'
        nProcessCount=0
        HOUSE_TYPE = '상업'
        strSwitchSidoCode=''
        strSwitchYYYYMM=''
        GOVMoltyAddressSequence='0'

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
            QuitException(GetLogDef.lineno(__file__)+ 'strResult => '+ strResult)  # 예외를 발생시킴

        if strResult == '10':
            QuitException(GetLogDef.lineno(__file__)+ 'It is currently in operation. => '+ strResult)  # 예외를 발생시킴

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

        qrySelectSeoulTradeMaster  = "SELECT * FROM " + ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable
        qrySelectSeoulTradeMaster += " WHERE state='00' AND dongmyun_code='00000' AND sigu_code!='000'"
        qrySelectSeoulTradeMaster += " AND seq >= "+GOVMoltyAddressSequence+" "
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

            #시작월 마지막 월 (12개월 * 30년)
            intRangeStart = int(intLoopStart)
            intRangeEnd = 12 * 2
            intRangeEnd = 2
            for nLoop in range(intRangeStart, intRangeEnd):
                # for nLoop in range(0, 730):

                print(GetLogDef.lineno(__file__), "[START LOOP]]================== ", nLoop)

                nbaseDate = dtToday - relativedelta(months=nLoop)
                dtProcessDay = str(int(nbaseDate.strftime("%Y%m")))

                print(GetLogDef.lineno(__file__), "dtProcessDay >> ", dtProcessDay)

                url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcNrgTrade'
                params = {'serviceKey': init_conf.MolitDecodedAuthorizationKey, 'LAWD_CD': strAdminSection,'DEAL_YMD': str(dtProcessDay)}

                # url = 'http://openapi.molit.go.kr/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTradeDev'
                # params = {'serviceKey':  init_conf.MolitDecodedAuthorizationKey , 'LAWD_CD': strAdminSection, 'DEAL_YMD': strDealYMD, 'pageNo': '2', 'numOfRows': '1000'}


                while True:
                    print(GetLogDef.lineno(__file__), "url===> ", strAdminSection, dtProcessDay, url)
                    print(GetLogDef.lineno(__file__), "params===> ", strAdminSection, dtProcessDay, params)
                    print(GetLogDef.lineno(__file__), "============================time.sleep(1) ")
                    time.sleep(1)
                    response = requests.get(url, params=params)
                    print(GetLogDef.lineno(__file__), "response===> ", type(response), response)
                    print(GetLogDef.lineno(__file__), "response.status_code===> ", type(response.status_code), response.status_code)
                    if response.status_code == int(200):
                        print(GetLogDef.lineno(__file__), "break ", type(response.raise_for_status()), response.raise_for_status())
                        responseContents = response.text  # page_source 얻기
                        ElementResponseRoot = ET.fromstring(responseContents)
                        strHeaderResultCode = ElementResponseRoot.find('header').find('resultCode').text
                        strHeaderResultMessage = ElementResponseRoot.find('header').find('resultMsg').text
                        print("strHeaderResultCode===> ", type(strHeaderResultCode), strHeaderResultCode)
                        print("strHeaderResultMessage===> ", type(strHeaderResultMessage), strHeaderResultMessage)

                        if strHeaderResultCode == '00':
                            print("url===> ", type(url), url)
                            print("params===> ", type(params), params)
                            break


                objectBodyItemAll = ElementResponseRoot.find('body').find('items')
                print(GetLogDef.lineno(__file__), "objectBodyItemAllCount >> ", len(objectBodyItemAll))
                # intGetCount = len(objectBodyItemAll)
                for objectBodyItem in objectBodyItemAll:
                    print(GetLogDef.lineno(__file__), "================objectBodyItem >> ", len(objectBodyItem) , objectBodyItem.tag)
                    print(GetLogDef.lineno(__file__), "================Text >> ", type( objectBodyItemAll.iter()) ,  objectBodyItemAll.iter())

                    for objectBodyItemITR in objectBodyItem.iter():
                        print(GetLogDef.lineno(__file__), "objectBodyItem.iter() >> ", objectBodyItemITR.tag ," =>" , objectBodyItemITR.text)



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
                    TOT_AREA=''
                    AGENT_ADDR=''
                    BJD_JIUN=''
                    SGG_CD=''
                    CANCEL_YN=''
                    CNTL_YMD=''
                    BONBEON=''
                    BUBEON = ''

                    if objectBodyItem.find('거래금액') != None:
                        OBJ_AMT = str(objectBodyItem.find('거래금액').text).strip().replace(",", "")
                    if objectBodyItem.find('거래유형') != None:
                        REQ_GBN = str(objectBodyItem.find('거래유형').text).strip()
                    if objectBodyItem.find('대지면적') != None:
                        TOT_AREA = str(objectBodyItem.find('대지면적').text).strip()
                    if objectBodyItem.find('건물면적') != None:
                        BLDG_AREA = str(objectBodyItem.find('건물면적').text).strip()
                    if objectBodyItem.find('건축년도') != None:
                        BUILD_YEAR = str(objectBodyItem.find('건축년도').text).strip()
                    if objectBodyItem.find('년') != None:
                        strTradeYYYY = str(objectBodyItem.find('년').text).strip().zfill(4)
                    if objectBodyItem.find('월') != None:
                        strTradeMM = str(objectBodyItem.find('월').text).strip().zfill(2)
                    if objectBodyItem.find('일') != None:
                        strTradeDD = str(objectBodyItem.find('일').text).strip().zfill(2)
                    if objectBodyItem.find('매도자') != None:
                        SELLER = str(objectBodyItem.find('매도자').text).strip()
                    if objectBodyItem.find('매수자') != None:
                        BUYER = str(objectBodyItem.find('매수자').text).strip()
                    if objectBodyItem.find('법정동') != None:
                        BJDONG_NM = str(objectBodyItem.find('법정동').text).strip()
                    if objectBodyItem.find('중개사소재지') != None:
                        AGENT_ADDR = str(objectBodyItem.find('중개사소재지').text).strip()
                    if objectBodyItem.find('지번') != None:
                        BJD_JIUN = str(objectBodyItem.find('지번').text).strip()
                    if objectBodyItem.find('지역코드') != None:
                        SGG_CD = str(objectBodyItem.find('지역코드').text).strip()
                    if objectBodyItem.find('해제여부') != None:
                        CANCEL_YN = str(objectBodyItem.find('해제여부').text).strip()
                    if objectBodyItem.find('해제사유발생일') != None:
                        CNTL_YMD = str(objectBodyItem.find('해제사유발생일').text).replace(".", "")
                    if objectBodyItem.find('층') != None:
                        FLOOR = str(objectBodyItem.find('층').text).strip()
                    if objectBodyItem.find('유형') != None:
                        HOUSE_TYPE = str(objectBodyItem.find('유형').text).strip().replace('\'',"")
                    if objectBodyItem.find('건물주용도') != None:
                        BLDG_NM = str(objectBodyItem.find('건물주용도').text).strip().replace('\'',"")
                    if objectBodyItem.find('용도지역') != None:
                        BLDG_DONG = str(objectBodyItem.find('용도지역').text).strip()


                    DEAL_YMD = strTradeYYYY + strTradeMM + strTradeDD

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



                    state = '00'
                    CNTL_YMD=''
                    if len(CANCEL_YN) > 0:
                        state = '10'
                        CNTL_YMD = "20" + CNTL_YMD


                    print(GetLogDef.lineno(__file__),"BJDONG_NM" , "["+BJDONG_NM+"]")

                    sqlSelectGOVCodeinfo  = " SELECT * FROM "+ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable+" WHERE sido_code='"+sido_code+"' AND  sigu_code='"+sigu_code+"' "
                    sqlSelectGOVCodeinfo += " AND dongmyun_name LIKE '%"+BJDONG_NM+"%'"
                    print(GetLogDef.lineno(__file__), "sqlSelectGOVCodeinfo =====> ", sqlSelectGOVCodeinfo ,sido_code , sigu_code )
                    cursorRealEstate.execute(sqlSelectGOVCodeinfo)
                    intGovCodeCount = cursorRealEstate.rowcount

                    if intGovCodeCount < 1:
                        BJDONG_NM = BJDONG_NM[0:-1]
                        print(GetLogDef.lineno(__file__), "intGovCodeCount =====> ", intGovCodeCount)
                        sqlSelectGOVCodeinfo  = " SELECT * FROM "+ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable+" WHERE sido_code='"+sido_code+"' AND  sigu_code='"+sigu_code+"' "
                        sqlSelectGOVCodeinfo += " AND dongmyun_name LIKE '%"+BJDONG_NM+"%'"
                        print(GetLogDef.lineno(__file__), "sqlSelectGOVCodeinfo =====> ", sqlSelectGOVCodeinfo ,sido_code , sigu_code )
                        cursorRealEstate.execute(sqlSelectGOVCodeinfo)
                        intGovCodeCount = cursorRealEstate.rowcount

                    if intGovCodeCount < 1:
                        print(GetLogDef.lineno(__file__), "intGovCodeCount =====> ", intGovCodeCount)
                        raise Exception("intGovCodeCount => " + str(intGovCodeCount))
                    elif intGovCodeCount > 1:

                        rstSelectDatas = cursorRealEstate.fetchall()
                        for rstSelectData in rstSelectDatas:
                            strGovInfoState = rstSelectData.get('state')
                            if strGovInfoState == '00':
                                BJDONG_CD = rstSelectData.get('dongmyun_code')
                                BJDONG_NM = rstSelectData.get('dongmyun_name')
                                SIDO_NM = rstSelectData.get('sido_name')
                                SGG_NM = rstSelectData.get('sigu_name')
                                break
                    else:
                        rstSelectDatas = cursorRealEstate.fetchone()
                        BJDONG_CD = rstSelectDatas.get('dongmyun_code')
                        BJDONG_NM = rstSelectDatas.get('dongmyun_name')
                        SIDO_NM = rstSelectDatas.get('sido_name')
                        SGG_NM = rstSelectDatas.get('sigu_name')

                    if len(BJDONG_CD) < 5:
                        print(GetLogDef.lineno(__file__), "BJDONG_CD =====> ", BJDONG_CD)
                        raise Exception("BJDONG_CD => " + str(BJDONG_CD))


                    strUniqueKey = strTradeYYYY + "_" +\
                                   sido_code + "_" +\
                                   sigu_code+ "_" +\
                                   BJDONG_CD + "_" + \
                                   BJD_JIUN + "_" + \
                                   DEAL_YMD + "_" + \
                                   BUILD_YEAR + "_" +OBJ_AMT

                    print(GetLogDef.lineno(__file__), "strUniqueKey" , strUniqueKey)

                    sqlSelectMOLIT = "SELECT * FROM "+ConstRealEstateTable_GOV.MolitBusinessRealTradeMasterTable+" WHERE unique_key = %s "
                    cursorRealEstate.execute(sqlSelectMOLIT , (strUniqueKey) )
                    intMolitCount = cursorRealEstate.rowcount
                    print(GetLogDef.lineno(__file__), "intMolitCount", intMolitCount)
                    if intMolitCount < 1:

                        #INSERT
                        sqlInsertMOLIT  = " INSERT INTO "+ConstRealEstateTable_GOV.MolitBusinessRealTradeMasterTable+" SET "
                        sqlInsertMOLIT += " unique_key = %s"
                        sqlInsertMOLIT += " , SIDO_CD = '"+sido_code+"'"
                        sqlInsertMOLIT += " , SIDO_NM = '"+SIDO_NM+"'"
                        sqlInsertMOLIT += " , SGG_CD = '"+sigu_code+"'"
                        sqlInsertMOLIT += " , SGG_NM = '"+SGG_NM+"'"
                        sqlInsertMOLIT += " , BJDONG_CD = '"+BJDONG_CD+"'"
                        sqlInsertMOLIT += " , BJDONG_NM = '"+BJDONG_NM+"'"
                        sqlInsertMOLIT += " , BONBEON = '"+BJD_JIUN+"'"
                        sqlInsertMOLIT += " , lng= '0' "
                        sqlInsertMOLIT += " , lat= '0' "
                        sqlInsertMOLIT += " , geo_point = ST_GeomFromText('POINT(0 0)', 4326,'axis-order=long-lat') "
                        sqlInsertMOLIT += " , BUILD_YEAR = '"+BUILD_YEAR+"'"
                        sqlInsertMOLIT += " , HOUSE_TYPE = '"+HOUSE_TYPE+"'"
                        sqlInsertMOLIT += " , DEAL_YMD = '"+DEAL_YMD+"'"
                        sqlInsertMOLIT += " , OBJ_AMT = '"+OBJ_AMT+"'"
                        sqlInsertMOLIT += " , TOT_AREA = '" + TOT_AREA + "'"
                        sqlInsertMOLIT += " , REQ_GBN = '"+REQ_GBN+"'"
                        sqlInsertMOLIT += " , SELLER = '" + SELLER + "'"
                        sqlInsertMOLIT += " , BUYER = '" + BUYER + "'"
                        sqlInsertMOLIT += " , AGENT_ADDR = '"+AGENT_ADDR+"'"
                        sqlInsertMOLIT += " , CNTL_YMD = '"+CNTL_YMD+"'"
                        sqlInsertMOLIT += " , state = '"+state+"'"
                        print(GetLogDef.lineno(__file__), "sqlInsertMOLIT ", sqlInsertMOLIT)
                        cursorRealEstate.execute(sqlInsertMOLIT , (strUniqueKey) )
                        ResRealEstateConnection.commit()
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

                        if DBstate == state:
                            print(GetLogDef.lineno(__file__), DBstate, " == ", state)
                            continue

                        sqlSelectMOLITCancel = "SELECT * FROM "+ConstRealEstateTable_GOV.MolitBusinessRealTradeCancelTable+" WHERE unique_key = %s "
                        cursorRealEstate.execute(sqlSelectMOLIT, (strUniqueKey))
                        intMolitCancelCount = cursorRealEstate.rowcount
                        if intMolitCancelCount < 0:
                            sqlInsertMOLITCancel  = " INSERT INTO "+ConstRealEstateTable_GOV.MolitBusinessRealTradeCancelTable+" SET "
                            sqlInsertMOLITCancel += " unique_key = %s"
                            sqlInsertMOLITCancel += " , SIDO_CD = '"+sido_code+"'"
                            sqlInsertMOLITCancel += " , SIDO_NM = '"+SIDO_NM+"'"
                            sqlInsertMOLITCancel += " , SGG_CD = '"+sigu_code+"'"
                            sqlInsertMOLITCancel += " , SGG_NM = '"+SGG_NM+"'"
                            sqlInsertMOLITCancel += " , BJDONG_CD = '"+BJDONG_CD+"'"
                            sqlInsertMOLITCancel += " , BJDONG_NM = '"+BJDONG_NM+"'"
                            sqlInsertMOLITCancel += " , BONBEON = '"+BJD_JIUN+"'"
                            sqlInsertMOLITCancel += " , lng= '0' "
                            sqlInsertMOLITCancel += " , lat= '0' "
                            sqlInsertMOLITCancel += " , geo_point = ST_GeomFromText('POINT(0 0)', 4326,'axis-order=long-lat') "
                            sqlInsertMOLITCancel += " , BUILD_YEAR = '"+BUILD_YEAR+"'"
                            sqlInsertMOLITCancel += " , HOUSE_TYPE = '"+HOUSE_TYPE+"'"
                            sqlInsertMOLITCancel += " , DEAL_YMD = '"+DEAL_YMD+"'"
                            sqlInsertMOLITCancel += " , OBJ_AMT = '"+OBJ_AMT+"'"
                            sqlInsertMOLITCancel += " , TOT_AREA = '" + TOT_AREA + "'"
                            sqlInsertMOLITCancel += " , REQ_GBN = '"+REQ_GBN+"'"
                            sqlInsertMOLITCancel += " , SELLER = '" + SELLER + "'"
                            sqlInsertMOLITCancel += " , BUYER = '" + BUYER + "'"
                            sqlInsertMOLITCancel += " , AGENT_ADDR = '"+AGENT_ADDR+"'"
                            sqlInsertMOLITCancel += " , CNTL_YMD = '"+CNTL_YMD+"'"
                            sqlInsertMOLITCancel += " , state = '"+state+"'"
                            print(GetLogDef.lineno(__file__), "sqlInsertMOLITCancel ", sqlInsertMOLITCancel)
                            cursorRealEstate.execute(sqlInsertMOLITCancel, (strUniqueKey))


                        sqlUpdateMOLIT = " UPDATE "+ConstRealEstateTable_GOV.MolitBusinessRealTradeMasterTable+" SET "
                        sqlUpdateMOLIT += " CNTL_YMD = '"+CNTL_YMD+"'"
                        sqlUpdateMOLIT += " , state = '"+state+"'"
                        sqlUpdateMOLIT += " , modify_date = NOW() "
                        sqlUpdateMOLIT += " WHERE unique_key = %s"
                        print(GetLogDef.lineno(__file__), "sqlUpdateMOLIT ", sqlUpdateMOLIT)
                        cursorRealEstate.execute(sqlUpdateMOLIT, (strUniqueKey) )
                        ResRealEstateConnection.commit()
                        nUpdateCount = nUpdateCount + 1

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
        dictSwitchData['data_4'] = nLoop
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