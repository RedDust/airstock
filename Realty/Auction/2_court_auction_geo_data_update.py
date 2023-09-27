
#https://console.ncloud.com/naver-service/application
#VPC AI·NAVER API Application

import requests


# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import datetime
sys.path.append("D:/PythonProjects/airstock")
import urllib.request
import traceback

from typing import Dict, Union, Optional

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV

from Const import ConstRealEstateTable_AUC
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Realty.Auction.Const import AuctionCourtInfo
from Init.DefConstant import ConstRealEstateTable
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable

try:
    print(GetLogDef.lineno(__file__), "============================================================")


    NAVER_API_URL: str = 'https://naveropenapi.apigw.ntruss.com/map-geocode/v2/geocode'
    NAVER_API_HEADERS: Dict[str, str] = {
        'X-NCP-APIGW-API-KEY-ID': 'idgprv275x',
        'X-NCP-APIGW-API-KEY': 'Ux9SxxEJiynvKt2CwKgfEOfXn6tA3ksyoVa4rAUs'
    }


    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
    qrySelectCourtAuctionMaster = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " WHERE process_step= '00'"
    # qrySelectCourtAuctionMaster = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionBackupTable + " WHERE process_step= '10'"
    # qrySelectCourtAuctionMaster += " limit 3"

    cursorRealEstate.execute(qrySelectCourtAuctionMaster)
    rstFieldsLists = cursorRealEstate.fetchall()
    # print(GetLogDef.lineno(__file__), "rstFieldsList>", rstFieldsLists)

    for SelectColumnList in rstFieldsLists:

        nSequence = SelectColumnList.get('seq')

        strFieldName = SelectColumnList.get('address_data')
        print(GetLogDef.lineno(__file__), "strFieldName >", type(strFieldName), strFieldName)
        jsonFieldName = json.loads(strFieldName)
        # print(GetLogDef.lineno(__file__), "jsonFieldName >", type(jsonFieldName), jsonFieldName)
        # print(GetLogDef.lineno(__file__), "jsonFieldName[0] >", type(jsonFieldName[0]), jsonFieldName[0])
        print(GetLogDef.lineno(__file__), "------------------------------------------------------------------------------------------------------------")
        print(GetLogDef.lineno(__file__), "nSequence>", nSequence, strFieldName,len(strFieldName))

        if len(strFieldName) < 10:
            #주소가 없을떄 업데이트 SKIP
            strJiBunAddress = ''
            strLongitude = '000.00000000'  # 127
            strLatitude = '000.00000000'  # 37
            nProcessStep = 12
            nTotalCount=0
        else:
            strTextAddress = (jsonFieldName[0])
            response = requests.get(
                NAVER_API_URL,
                params={'query': strTextAddress},
                headers=NAVER_API_HEADERS
            )

            results = response.json()
            print(GetLogDef.lineno(__file__), "results", type(results), results)

            if results['status'] != 'OK':
                Exception(GetLogDef.lineno(__file__), "results['status']  => ", results['status'] )  # 예외를 발생시킴
                break

            print(GetLogDef.lineno(__file__), "results['meta']>", type(results['meta']), results['meta'])
            print(GetLogDef.lineno(__file__), "nTotalCount>", type(nTotalCount), nTotalCount)
            print(GetLogDef.lineno(__file__), "results['addresses']>", type(results['addresses']), results['addresses'])
            nTotalCount = results['meta']['totalCount']

            if nTotalCount < 1:
                strJiBunAddress = ''
                strLongitude = '000.00000000'  # 127
                strLatitude = '000.00000000'  # 37
                nProcessStep = 11
            else:
                dictAddresses = results['addresses']
                listFirstAddress = dictAddresses[0]
                strJiBunAddress = listFirstAddress['jibunAddress']
                strLongitude = listFirstAddress['x']  # 127
                strLatitude = listFirstAddress['y']  # 37
                nProcessStep = 10


        print(GetLogDef.lineno(__file__), "strJiBunAddress>",type(strJiBunAddress), strJiBunAddress)
        print(GetLogDef.lineno(__file__), "strLongitude>",type(strLongitude), strLongitude)
        print(GetLogDef.lineno(__file__), "strLatitude>",type(strLatitude), strLatitude)
        print(GetLogDef.lineno(__file__), "nSequence>",type(nSequence), nSequence)

        qryUpdateGeoPosition = " UPDATE " + ConstRealEstateTable_AUC.CourtAuctionDataTable + " SET "
        qryUpdateGeoPosition += " text_address = %s, "
        qryUpdateGeoPosition += " longitude = %s, "
        qryUpdateGeoPosition += " latitude = %s, "
        qryUpdateGeoPosition += " geo_point = ST_GeomFromText('POINT("+strLongitude+" "+strLatitude+")'), "
        qryUpdateGeoPosition += " process_step = %s "
        qryUpdateGeoPosition += " WHERE seq = %s "

        print("qryInfoUpdate", qryUpdateGeoPosition, type(qryUpdateGeoPosition))
        cursorRealEstate.execute(qryUpdateGeoPosition, (strJiBunAddress, strLongitude, strLatitude, nProcessStep, nSequence))

        ResRealEstateConnection.commit()

    # qrySelectCourtAuctionBackup = " SELECT * FROM " + ConstRealEstateTable_AUC.CourtAuctionBackupTable + " "

    #
    #
    # response = requests.get(
    #     NAVER_API_URL,
    #     params={'query': strTextAddress },
    #     headers=NAVER_API_HEADERS
    # )
    #
    # results = response.json()
    # print(results, type(results))



    #
    #
    #
    # dictAaa = results['addresses']
    # dictAddress = dictAaa[0]
    #
    # strJiBunAddress = dictAddress['jibunAddress']
    # strLongitude = dictAddress['x'] # 127
    # strLatitude = dictAddress['y'] # 37
    #
    # qryUpdateGeoPosition = "UPDATE SET"
    #
    # print(dictAddress['jibunAddress'])
    # print(GetLogDef.lineno(__file__), type(strJiBunAddress), strJiBunAddress, "============================================================")
    # print(GetLogDef.lineno(__file__), type(strLongitude), strLongitude, "============================================================")
    # print(GetLogDef.lineno(__file__), type(strLatitude), strLatitude, "============================================================")


    # aList = json.loads(results)
    # aList = json.dumps(results)


    # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.


except Exception as e:

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '30'

    # if KuIndex is not None:
    #     dictSwitchData['data_1'] = KuIndex
    #
    # if arrCityPlace is not None:
    #     dictSwitchData['data_2'] = arrCityPlace
    #
    # if targetRow is not None:
    #     dictSwitchData['data_3'] = targetRow
    #
    # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    print(GetLogDef.lineno(__file__), "Error Exception")
    print(GetLogDef.lineno(__file__), e, type(e))
    err_msg = traceback.format_exc()
    print(err_msg)

else:
    print(GetLogDef.lineno(__file__), "============================================================")

finally:
    print("Finally END")

