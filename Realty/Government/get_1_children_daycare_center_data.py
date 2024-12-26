#어린이집별 기본정보 조회
#https://info.childcare.go.kr/info_html5/oais/openapi/OpenApiInfoSl.jsp
#SERVICE URL http://api.childcare.go.kr/mediate/rest/cpmsapi030/cpmsapi030/request

import sys
sys.path.append("D:/PythonProjects/airstock")

#DAYCARE_AUTH_KEY = '36b8462042b14048a48e980b8a846f0b'

#개발 계정 인증키
DAYCARE_AUTH_KEY = '36b8462042b14048a48e980b8a846f0b'


#ConstRealEstateTable_GOV.GOVMoltyAddressInfoTable = 'kt_realty_gov_code_info'
#ConstRealEstateTable_GOV.ChildrenDaycareCenterMasterTable = 'kt_realty_children_daycare_center_master'


import urllib.request
import json
import pymysql
import traceback
import time
import re
import pandas as pd
import requests
from pandas.io.json import json_normalize
from Realty.Government.Init import init_conf
from Lib.RDB import pyMysqlConnector
from dateutil.relativedelta import relativedelta

from Init.Functions.Logs import GetLogDef

from Init.DefConstant import ConstRealEstateTable
from Realty.Government.Const import ConstRealEstateTable_GOV

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
from bs4 import BeautifulSoup
import xml
import xml.etree.ElementTree as ET
from html.parser import HTMLParser

from Lib.CustomException import QuitException
import requests

def main():

    try:

        #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
        #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
        #거래 신고 30일 + 취소 신고 +30일
        stToday = DateTime.today()
        DAYCARE_AUTH_KEY = '528c41f06c8146b09cae6b37431f7a66'
        nInsertedCount = 0
        nUpdateCount = 0

        #서울 부동산 실거래가 데이터 - 매매
        strProcessType = '041101'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'
        nProcessCount=0

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
            QuitException.QuitException(GetLogDef.lineno(__file__)+ 'strResult => '+ strResult)  # 예외를 발생시킴

        if strResult == '10':
            QuitException.QuitException(GetLogDef.lineno(__file__)+ 'It is currently in operation. => '+ strResult)  # 예외를 발생시킴

        if strResult == '20':
            GOVMoltyAddressSequence = str(rstResult.get('data_3'))


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)


        qrySelectSeoulTradeMaster  = "SELECT * FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
        qrySelectSeoulTradeMaster += " WHERE state='00' AND sgg_cd<>'000' AND umd_cd='000' AND ri_cd='00'"
        qrySelectSeoulTradeMaster += " AND seq >= "+GOVMoltyAddressSequence+" "
        qrySelectSeoulTradeMaster += " ORDER BY seq ASC "
        # qrySelectSeoulTradeMaster += " LIMIT 1 "
        print(GetLogDef.lineno(__file__), "qrySelectSeoulTradeMaster >> ", qrySelectSeoulTradeMaster)
        cursorRealEstate.execute(qrySelectSeoulTradeMaster)
        row_result = cursorRealEstate.rowcount
        # 등록되어 있는 물건이면 패스


        print(GetLogDef.lineno(__file__), "row_result >> " , row_result)
        rstSelectDatas = cursorRealEstate.fetchall()

        for rstSelectData in rstSelectDatas:

            GOVMoltyAddressSequence = str(rstSelectData.get('seq'))
            print(GetLogDef.lineno(__file__), "GOVMoltyAddressSequence >> ", GOVMoltyAddressSequence)

            sido_code = str(rstSelectData.get('sido_cd')).zfill(2)
            sigu_code = rstSelectData.get('sgg_cd').zfill(3)

            sido_name = str(rstSelectData.get('locatadd_nm'))

            strAdminSection =  sido_code+sigu_code
            strAdminName = sido_name

            print(GetLogDef.lineno(__file__), "strAdminName >> ", strAdminName)
            print(GetLogDef.lineno(__file__), "strAdminSection >> ", strAdminSection)

            dtToday = DateTime.now()

            #시작월 마지막 월 (12개월 * 30년)

            print(GetLogDef.lineno(__file__), "[START strAdminName]]================== ", strAdminName)

            url = 'http://api.childcare.go.kr/mediate/rest/cpmsapi030/cpmsapi030/request'

            params = {'key': DAYCARE_AUTH_KEY, 'arcode': strAdminSection}
            # response = requests.get(url, params=params)
            # print("response===> ", type(response),  response )
            # print("response.status_code===> ", type(response.status_code), response.status_code)
            # print("response.raise_for_status()===> ", type(response.raise_for_status()), response.raise_for_status())

            # if response.status_code != 200:
            print(GetLogDef.lineno(__file__), "url===> ", strAdminSection,  url)
            print(GetLogDef.lineno(__file__), "params===> ", strAdminSection,  params)
            response = requests.get(url, params=params)
            # print(GetLogDef.lineno(__file__), "response===> ", type(response), response)
            if response.status_code != int(200):
                print(GetLogDef.lineno(__file__), "break ", type(response.status_code), response.status_code)
                time.sleep(10)
                response = requests.get(url, params=params)

                if response.status_code != int(200):
                    print(GetLogDef.lineno(__file__), "break ", type(response.status_code), response.status_code)
                    continue

            responseContents = response.text  # page_source 얻기

            soup = BeautifulSoup(responseContents, "html.parser")  # get html

            rstTrElements = soup.find_all('item')

            # print(GetLogDef.lineno(__file__), "rstTrElements===> ", rstTrElements)

            for rstTrElement in rstTrElements:
                print(GetLogDef.lineno(__file__), "rstTrElement===> ", type(rstTrElement), rstTrElement )

                strSidoName = rstTrElement.find('sidoname').text
                strSigunName = rstTrElement.find('sigunname').text
                strStCode = rstTrElement.find('stcode').text
                strCrName = GetLogDef.stripSpecharsForText(rstTrElement.find('crname').text)
                strCrTypeName = rstTrElement.find('crtypename').text
                strCrStatusName = rstTrElement.find('crstatusname').text
                strZipCode = rstTrElement.find('zipcode').text
                strCrAddr = GetLogDef.stripSpecharsForText(rstTrElement.find('craddr').text)
                strCrTelno = rstTrElement.find('crtelno').text
                strCrFaxno = rstTrElement.find('crfaxno').text
                strCrHome = GetLogDef.stripSpecharsForText(rstTrElement.find('crhome').text)
                strNrtrRoomCnt = re.sub(r'[^0-9]', '', rstTrElement.find('nrtrroomcnt').text).zfill(1)
                strNrtrRoomSize = re.sub(r'[^0-9]', '', rstTrElement.find('nrtrroomsize').text).zfill(1)
                strPlgrdco = re.sub(r'[^0-9]', '', rstTrElement.find('plgrdco').text).zfill(1)
                strCctvInstlCnt = re.sub(r'[^0-9]', '', rstTrElement.find('cctvinstlcnt').text).zfill(1)
                strChcrTesCnt = re.sub(r'[^0-9]', '', rstTrElement.find('chcrtescnt').text).zfill(1)
                strCrCapat = re.sub(r'[^0-9]', '', rstTrElement.find('crcapat').text).zfill(1)
                strCrchCnt = re.sub(r'[^0-9]', '', rstTrElement.find('crchcnt').text).zfill(1)
                strLatitude = str(rstTrElement.find('la').text).zfill(1)
                strLongitude = str(rstTrElement.find('lo').text).zfill(1)
                strCrcargbName = rstTrElement.find('crcargbname').text
                strCrcnfmdt = rstTrElement.find('crcnfmdt').text
                strCrPauseBeginDt = rstTrElement.find('crpausebegindt').text
                strCrPauseEndDt = rstTrElement.find('crpauseenddt').text
                strCrablDt = rstTrElement.find('crabldt').text
                strDataStdrDt = rstTrElement.find('datastdrdt').text
                strCrSpec = rstTrElement.find('crspec').text
                strCrRepName = rstTrElement.find('crrepname').text
                strClassCnt_00 = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_00').text).zfill(1)
                strClassCnt_01 = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_01').text).zfill(1)
                strClassCnt_02 = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_02').text).zfill(1)
                strClassCnt_03 = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_03').text).zfill(1)
                strClassCnt_04 = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_04').text).zfill(1)
                strClassCnt_05 = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_05').text).zfill(1)
                strClassCnt_M2 = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_m2').text).zfill(1)
                strClassCnt_M5 = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_m5').text).zfill(1)
                strClassCnt_SP = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_sp').text).zfill(1)
                strClassCnt_TOT = re.sub(r'[^0-9]', '', rstTrElement.find('class_cnt_tot').text).zfill(1)
                strChildCnt_00 = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_00').text).zfill(1)
                strChildCnt_01 = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_01').text).zfill(1)
                strChildCnt_02 = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_02').text).zfill(1)
                strChildCnt_03 = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_03').text).zfill(1)
                strChildCnt_04 = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_04').text).zfill(1)
                strChildCnt_05 = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_05').text).zfill(1)
                strChildCnt_M2 = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_m2').text).zfill(1)
                strChildCnt_M5 = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_m5').text).zfill(1)
                strChildCnt_SP = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_sp').text).zfill(1)
                strChildCnt_TOT = re.sub(r'[^0-9]', '', rstTrElement.find('child_cnt_tot').text).zfill(1)
                strEmCnt_0Y = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_0y').text).zfill(1)
                strEmCnt_1Y = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_1y').text).zfill(1)
                strEmCnt_2Y = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_2y').text).zfill(1)
                strEmCnt_4Y = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_4y').text).zfill(1)
                strEmCnt_6Y = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_6y').text).zfill(1)
                strEmCnt_A1 = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_a1').text).zfill(1)
                strEmCnt_A2 = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_a2').text).zfill(1)
                strEmCnt_A3 = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_a3').text).zfill(1)
                strEmCnt_A4 = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_a4').text).zfill(1)
                strEmCnt_A5 = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_a5').text).zfill(1)
                strEmCnt_A6 = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_a6').text).zfill(1)
                strEmCnt_A7 = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_a7').text).zfill(1)
                strEmCnt_A8 = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_a8').text).zfill(1)
                strEmCnt_A10 = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_a10').text).zfill(1)
                strEmCnt_TOT = re.sub(r'[^0-9]', '', rstTrElement.find('em_cnt_tot').text).zfill(1)
                strEwCnt_00 = re.sub(r'[^0-9]', '', rstTrElement.find('ew_cnt_00').text).zfill(1)
                strEwCnt_01 = re.sub(r'[^0-9]', '', rstTrElement.find('ew_cnt_01').text).zfill(1)
                strEwCnt_02 = re.sub(r'[^0-9]', '', rstTrElement.find('ew_cnt_02').text).zfill(1)
                strEwCnt_03 = re.sub(r'[^0-9]', '', rstTrElement.find('ew_cnt_03').text).zfill(1)
                strEwCnt_04 = re.sub(r'[^0-9]', '', rstTrElement.find('ew_cnt_04').text).zfill(1)
                strEwCnt_05 = re.sub(r'[^0-9]', '', rstTrElement.find('ew_cnt_05').text).zfill(1)
                strEwCnt_M6 = re.sub(r'[^0-9]', '', rstTrElement.find('ew_cnt_m6').text).zfill(1)
                strEwCnt_TOT = re.sub(r'[^0-9]', '', rstTrElement.find('ew_cnt_tot').text).zfill(1)


                strUniqueKey = strStCode + strZipCode + strDataStdrDt

                sqlSelectDaycareCenterMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.ChildrenDaycareCenterMasterTable
                sqlSelectDaycareCenterMaster += " WHERE unique_key = %s "
                print(GetLogDef.lineno(__file__), "SELECT =>", sqlSelectDaycareCenterMaster, (strUniqueKey))
                cursorRealEstate.execute(sqlSelectDaycareCenterMaster, strUniqueKey)
                row_result = cursorRealEstate.rowcount

                # 등록되어 있는 물건이면 패스


                #이미 저장 되어 있으면 더이상 저장 하지 않는다.
                if row_result > 0:
                    print(GetLogDef.lineno(__file__), "EXIST =>", sqlSelectDaycareCenterMaster, strUniqueKey)
                    nUpdateCount = nUpdateCount + 1
                    continue


                sqlInsertDaycareCenterBackup = " INSERT INTO " + ConstRealEstateTable_GOV.ChildrenDaycareCenterBackupTable
                sqlInsertDaycareCenterBackup += " (SELECT * FROM " + ConstRealEstateTable_GOV.ChildrenDaycareCenterMasterTable
                sqlInsertDaycareCenterBackup += " WHERE unique_key=%s"
                sqlInsertDaycareCenterBackup += ")"
                print(GetLogDef.lineno(__file__), "BACKUP =>", sqlInsertDaycareCenterBackup, (strUniqueKey))
                cursorRealEstate.execute(sqlInsertDaycareCenterBackup, (strUniqueKey))

                # sqlDeleteDaycareCenterUpdate = " DELETE FROM "  + ConstRealEstateTable_GOV.ChildrenDaycareCenterMasterTable
                # sqlDeleteDaycareCenterUpdate += " WHERE stcode=%s "
                # print(GetLogDef.lineno(__file__), "DELETE =>", sqlDeleteDaycareCenterUpdate,strStCode,strDataStdrDt)
                # cursorRealEstate.execute(sqlDeleteDaycareCenterUpdate, (strStCode))

                sqlModifyDaycareCenterMaster = " INSERT INTO " + ConstRealEstateTable_GOV.ChildrenDaycareCenterMasterTable + " SET "
                sqlModifyDaycareCenterMaster += " unique_key ='"+strUniqueKey+"' "
                sqlModifyDaycareCenterMaster += " ,sidoname = '"+strSidoName+"' "
                sqlModifyDaycareCenterMaster += " ,sigunguname = '" + strSigunName + "' "
                sqlModifyDaycareCenterMaster += " ,stcode = '" + strStCode + "' "
                sqlModifyDaycareCenterMaster += " ,crname = '" + strCrName + "' "
                sqlModifyDaycareCenterMaster += " ,crtypename = '" + strCrTypeName + "' "
                sqlModifyDaycareCenterMaster += " ,crstatusname = '" + strCrStatusName + "' "
                sqlModifyDaycareCenterMaster += " ,zipcode = '" + strZipCode + "' "
                sqlModifyDaycareCenterMaster += " ,craddr = '" + strCrAddr + "' "
                sqlModifyDaycareCenterMaster += " ,crtelno = '" + strCrTelno + "' "
                sqlModifyDaycareCenterMaster += " ,crfaxno = '" + strCrFaxno + "' "
                sqlModifyDaycareCenterMaster += " ,crhome = '" + strCrHome + "' "
                sqlModifyDaycareCenterMaster += " ,nrtrroomcnt = '" + strNrtrRoomCnt + "' "
                sqlModifyDaycareCenterMaster += " ,nrtrroomsize = '" + strNrtrRoomSize + "' "
                sqlModifyDaycareCenterMaster += " ,plgrdco = '" + strPlgrdco + "' "
                sqlModifyDaycareCenterMaster += " ,cctvinstlcnt = '" + strCctvInstlCnt + "' "
                sqlModifyDaycareCenterMaster += " ,chcrtescnt = '" + strChcrTesCnt + "' "
                sqlModifyDaycareCenterMaster += " ,crcapat = '" + strCrCapat + "' "
                sqlModifyDaycareCenterMaster += " ,crchcnt = '" + strCrchCnt + "' "
                sqlModifyDaycareCenterMaster += " ,lng = '" + strLongitude + "' "
                sqlModifyDaycareCenterMaster += " ,lat = '" + strLatitude + "' "
                sqlModifyDaycareCenterMaster += " ,crcargbname = '" + strCrcargbName + "' "
                sqlModifyDaycareCenterMaster += " ,crcnfmdt = '" + strCrcnfmdt + "' "
                sqlModifyDaycareCenterMaster += " ,crpausebegindt = '" + strCrPauseBeginDt + "' "
                sqlModifyDaycareCenterMaster += " ,crpauseenddt = '" + strCrPauseEndDt + "' "
                sqlModifyDaycareCenterMaster += " ,crabldt = '" + strCrablDt + "' "
                sqlModifyDaycareCenterMaster += " ,datastdrdt = '" + strDataStdrDt + "' "
                sqlModifyDaycareCenterMaster += " ,crspec = '" + strCrSpec + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_00 = '" + strClassCnt_00 + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_01 = '" + strClassCnt_01 + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_02 = '" + strClassCnt_02 + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_03 = '" + strClassCnt_03 + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_04 = '" + strClassCnt_04 + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_05 = '" + strClassCnt_05 + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_m2 = '" + strClassCnt_M2 + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_m5 = '" + strClassCnt_M5 + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_sp = '" + strClassCnt_SP + "' "
                sqlModifyDaycareCenterMaster += " ,class_cnt_tot = '" + strClassCnt_TOT + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_00 = '" + strChildCnt_00 + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_01 = '" + strChildCnt_01 + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_02 = '" + strChildCnt_02 + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_03 = '" + strChildCnt_03 + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_04 = '" + strChildCnt_04 + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_05 = '" + strChildCnt_05 + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_m2 = '" + strChildCnt_M2 + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_m5 = '" + strChildCnt_M5 + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_sp = '" + strChildCnt_SP + "' "
                sqlModifyDaycareCenterMaster += " ,child_cnt_tot = '" + strChildCnt_TOT + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_0y = '" + strEmCnt_0Y + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_1y = '" + strEmCnt_1Y + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_2y = '" + strEmCnt_2Y + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_4y = '" + strEmCnt_4Y + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_6y = '" + strEmCnt_6Y + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_a1 = '" + strEmCnt_A1 + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_a2 = '" + strEmCnt_A2 + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_a3 = '" + strEmCnt_A3 + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_a4 = '" + strEmCnt_A4 + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_a5 = '" + strEmCnt_A5 + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_a6 = '" + strEmCnt_A6 + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_a7 = '" + strEmCnt_A7 + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_a8 = '" + strEmCnt_A8 + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_a10 = '" + strEmCnt_A10 + "' "
                sqlModifyDaycareCenterMaster += " ,em_cnt_tot = '" + strEmCnt_TOT + "' "
                sqlModifyDaycareCenterMaster += " ,ew_cnt_00 = '" + strEwCnt_00 + "' "
                sqlModifyDaycareCenterMaster += " ,ew_cnt_01 = '" + strEwCnt_01 + "' "
                sqlModifyDaycareCenterMaster += " ,ew_cnt_02 = '" + strEwCnt_02 + "' "
                sqlModifyDaycareCenterMaster += " ,ew_cnt_03 = '" + strEwCnt_03 + "' "
                sqlModifyDaycareCenterMaster += " ,ew_cnt_04 = '" + strEwCnt_04 + "' "
                sqlModifyDaycareCenterMaster += " ,ew_cnt_05 = '" + strEwCnt_05 + "' "
                sqlModifyDaycareCenterMaster += " ,ew_cnt_m6 = '" + strEwCnt_M6 + "' "
                sqlModifyDaycareCenterMaster += " ,ew_cnt_tot = '" + strEwCnt_TOT + "' "
                sqlModifyDaycareCenterMaster += " ,Crrepname = '" + strCrRepName + "' "
                sqlModifyDaycareCenterMaster += " ,longitude = '" + strLongitude + "' "
                sqlModifyDaycareCenterMaster += " ,latitude = '" + strLatitude + "' "
                sqlModifyDaycareCenterMaster += " ,geo_point = ST_GeomFromText('POINT(" + strLongitude + " " + strLatitude + ")', 4326,'axis-order=long-lat') "
                print(GetLogDef.lineno(__file__), "INSERT =>", sqlModifyDaycareCenterMaster)

                cursorRealEstate.execute(sqlModifyDaycareCenterMaster)
                ResRealEstateConnection.commit()



                nInsertedCount = nInsertedCount + 1

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = strUniqueKey
            dictSwitchData['data_2'] = strSidoName + " " + strSigunName
            dictSwitchData['data_3'] = GOVMoltyAddressSequence
            dictSwitchData['data_4'] = strCrName
            dictSwitchData['data_5'] = nUpdateCount
            dictSwitchData['data_6'] = nInsertedCount
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            print(GetLogDef.lineno(__file__), "[END strSidoName]]================== ", strSidoName)
            print(GetLogDef.lineno(__file__), "[END strSigunName]]================== ", strSigunName)
            print(GetLogDef.lineno(__file__), "[END strCrName]]================== ", strCrName)

            time.sleep(2)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = strUniqueKey
        dictSwitchData['data_2'] = strSidoName
        dictSwitchData['data_3'] = GOVMoltyAddressSequence
        dictSwitchData['data_4'] = strCrName
        dictSwitchData['data_5'] = nUpdateCount
        dictSwitchData['data_6'] = nInsertedCount
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.lineno(__file__), "[END strAdminName]]================== ", strAdminName)


    except Exception as e:

        print(GetLogDef.lineno(__file__), "Error Exception")
        print(GetLogDef.lineno(__file__), e)
        print(GetLogDef.lineno(__file__), type(e))
        err_msg = traceback.format_exc()
        print(GetLogDef.lineno(__file__), err_msg)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'
        dictSwitchData['data_3'] = GOVMoltyAddressSequence
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.lineno(__file__), "[END strAdminName]]================== ", strAdminName)

    else:
        print(GetLogDef.lineno(__file__), 'Inserted => ', nInsertedCount, ' , Updated => ', nUpdateCount)
        print(GetLogDef.lineno(__file__), "========================= SUCCESS END")
    finally:
        print(GetLogDef.lineno(__file__), "Finally END")
