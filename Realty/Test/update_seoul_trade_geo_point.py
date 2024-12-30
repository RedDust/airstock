# 서울 일별 전월세 데이터 수집 프로그램
# 2023-01-30 커밋
#https://data.seoul.go.kr/dataList/OA-21276/S/1/datasetView.do


import sys
sys.path.append("D:/PythonProjects/airstock")


import urllib.request
import json
import pymysql
import datetime

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV


from Lib.RDB import pyMysqlConnector

from Init.Functions.Logs import GetLogDef
from Lib.GeoDataModule import GeoDataModule

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
import time
import random


try:
    #사용변수 초기화
    nSequence = 0

    #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
    #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
    #거래 신고 30일 + 취소 신고 +30일
    dtStartDateTime = DateTime.today()

    nTotalCount = 0
    nProcessCount = 0
    nTradeCount = 0
    nRentCount = 0
    nLookIntoCount = 0
    nNoLookCount = 0


    # DB 연결
    ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()

    cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
    qrySelectSeoulTradeMaster  = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + " WHERE lat='' "
    qrySelectSeoulTradeMaster += " AND HOUSE_TYPE != '단독다가구' "
    # qrySelectSeoulTradeMaster += " AND CNTRCT_DE > '20170101' "
    # qrySelectSeoulTradeMaster += " ORDER BY seq DESC "
    # qrySelectSeoulTradeMaster += " LIMIT 1000000 "

    cursorRealEstate.execute(qrySelectSeoulTradeMaster)
    nTotalCount = row_result = cursorRealEstate.rowcount
    # 등록되어 있는 물건이면 패스

    rstSelectDatas = cursorRealEstate.fetchall()
    for rstSelectData in rstSelectDatas:
        nProcessCount += 1

        strDBSequence = str(rstSelectData.get('seq'))
        strDBHouseType = str(rstSelectData.get('HOUSE_TYPE'))
        strDBSGG_NM = str(rstSelectData.get('SGG_NM'))
        strDBBJDONG_NM = str(rstSelectData.get('BJDONG_NM'))
        strDBBOBN = str(rstSelectData.get('BONBEON'))
        strDBBUBN = str(rstSelectData.get('BUBEON'))
        strDBBLDG_NM = str(rstSelectData.get('BLDG_NM'))

        print(GetLogDef.lineno(__file__), "------------------------------------------------------------------------------")
        print(GetLogDef.lineno(__file__), "[ strDBSequence >  ]["+str(nProcessCount)+" / "+str(nTotalCount)+"][ " + str(strDBSequence) + " ] ")

        print(GetLogDef.lineno(__file__), "strDBHouseType" , strDBHouseType)

        strTradeDBMasterBUBEON = str(strDBBUBN).lstrip("0")
        if strTradeDBMasterBUBEON != '':
            strTradeDBMasterBUBEON = "-" + strTradeDBMasterBUBEON

        qrySelectSeoulRentMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + " "
        qrySelectSeoulRentMaster += " WHERE SGG_NM=%s "
        qrySelectSeoulRentMaster += " AND BJDONG_NM=%s "
        qrySelectSeoulRentMaster += " AND BONBEON=%s "
        qrySelectSeoulRentMaster += " AND BUBEON=%s "
        qrySelectSeoulRentMaster += " AND lng!='' "
        qrySelectSeoulRentMaster += " AND lat!='' "
        qrySelectSeoulRentMaster += " LIMIT 1 "

        print(GetLogDef.lineno(__file__), "qrySelectSeoulRentMaster >", qrySelectSeoulRentMaster, strDBSGG_NM, strDBBJDONG_NM, strDBBOBN, strDBBUBN)
        cursorRealEstate.execute(qrySelectSeoulRentMaster, ( strDBSGG_NM, strDBBJDONG_NM, strDBBOBN, strDBBUBN))
        intSelectResult = cursorRealEstate.rowcount
        if intSelectResult > 0:

            rstSelectData = cursorRealEstate.fetchone()
            strCancelState = rstSelectData.get('state')
            strNaverLongitude = str(float(rstSelectData.get('lng')))  # 127
            strNaverLatitude = str(float(rstSelectData.get('lat')))  # 37
            print(GetLogDef.lineno(__file__), "strNaverLongitude >", strNaverLongitude, strNaverLatitude)

            sqlUpdateSeoulRealRent = "UPDATE " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + " SET  modify_date = NOW()"
            sqlUpdateSeoulRealRent += " , lng='" + strNaverLongitude + "' "
            sqlUpdateSeoulRealRent += " , lat='" + strNaverLatitude + "' "
            sqlUpdateSeoulRealRent += " , geo_point = ST_GeomFromText('POINT(" + strNaverLongitude + " " + strNaverLatitude + ")', 4326,'axis-order=long-lat')"
            sqlUpdateSeoulRealRent += " WHERE seq=%s "

            print(GetLogDef.lineno(__file__), "COMMIT > ", sqlUpdateSeoulRealRent, strDBSequence)
            cursorRealEstate.execute(sqlUpdateSeoulRealRent, (strDBSequence))
            ResRealEstateConnection.commit()

            nTradeCount +=1

            continue


        qrySelectSeoulRentMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulRealRentDataTable + " "
        qrySelectSeoulRentMaster += " WHERE SGG_NM=%s "
        qrySelectSeoulRentMaster += " AND BJDONG_NM=%s "
        qrySelectSeoulRentMaster += " AND BOBN=%s "
        qrySelectSeoulRentMaster += " AND BUBN=%s "
        qrySelectSeoulRentMaster += " AND lng!='' "
        qrySelectSeoulRentMaster += " AND lat!='' "
        qrySelectSeoulRentMaster += " LIMIT 1 "
        cursorRealEstate.execute(qrySelectSeoulRentMaster, ( strDBSGG_NM, strDBBJDONG_NM, strDBBOBN, strDBBUBN))
        intSelectResult = cursorRealEstate.rowcount
        if intSelectResult > 0:

            rstSelectData = cursorRealEstate.fetchone()
            strCancelState = rstSelectData.get('state')
            strNaverLongitude = str(float(rstSelectData.get('lng')))  # 127
            strNaverLatitude = str(float(rstSelectData.get('lat')))  # 37
            print(GetLogDef.lineno(__file__), "strNaverLongitude >", strNaverLongitude, strNaverLatitude)

            qrySelectSeoulRentMaster = "UPDATE " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + " SET  modify_date = NOW()"
            qrySelectSeoulRentMaster += " , lng='" + strNaverLongitude + "' "
            qrySelectSeoulRentMaster += " , lat='" + strNaverLatitude + "' "
            qrySelectSeoulRentMaster += " , geo_point = ST_GeomFromText('POINT(" + strNaverLongitude + " " + strNaverLatitude + ")', 4326,'axis-order=long-lat')"
            qrySelectSeoulRentMaster += " WHERE SGG_NM=%s "
            qrySelectSeoulRentMaster += " AND BJDONG_NM=%s "
            qrySelectSeoulRentMaster += " AND BONBEON=%s "
            qrySelectSeoulRentMaster += " AND BUBEON=%s "

            print(GetLogDef.lineno(__file__), "COMMIT > ", qrySelectSeoulRentMaster, strDBSequence)
            cursorRealEstate.execute(qrySelectSeoulRentMaster, (strDBSGG_NM, strDBBJDONG_NM, strDBBOBN, strDBBUBN))
            ResRealEstateConnection.commit()

            nRentCount +=1

            continue






        strDOROJUSO = "서울특별시 "
        strDOROJUSO += strDBSGG_NM + " "
        strDOROJUSO += strDBBJDONG_NM + " "
        strDOROJUSO += str(strDBBOBN).lstrip("0")
        strDOROJUSO += strTradeDBMasterBUBEON
        print(GetLogDef.lineno(__file__), "strDOROJUSO => ", strDOROJUSO)

        resultsDict = GeoDataModule.getJusoData(strDOROJUSO)
        print(GetLogDef.lineno(__file__), "resultsDict >", type(resultsDict), isinstance(resultsDict, dict),
              resultsDict)
        if isinstance(resultsDict, dict) == True:
            print(GetLogDef.lineno(__file__), resultsDict['jibunAddr'])
            strDOROJUSO = str(resultsDict['roadAddrPart1']).strip()

        resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
        print(GetLogDef.lineno(__file__), "resultsDict >", type(resultsDict), resultsDict)
        if isinstance(resultsDict, dict) == False:
            nNoLookCount += 1
            continue



        strNaverLongitude = str(resultsDict['x'])  # 127
        strNaverLatitude = str(resultsDict['y'])  # 37

        sqlUpdateSeoulRealRent  = "UPDATE " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + " SET  modify_date = NOW()"
        sqlUpdateSeoulRealRent += " , lng='" + strNaverLongitude + "' "
        sqlUpdateSeoulRealRent += " , lat='" + strNaverLatitude + "' "
        sqlUpdateSeoulRealRent += " , geo_point = ST_GeomFromText('POINT(" + strNaverLongitude + " " + strNaverLatitude + ")', 4326,'axis-order=long-lat')"
        sqlUpdateSeoulRealRent += " WHERE seq=%s "

        print(GetLogDef.lineno(__file__), "COMMIT > ", sqlUpdateSeoulRealRent, strDBSequence)
        cursorRealEstate.execute(sqlUpdateSeoulRealRent , (strDBSequence) )
        ResRealEstateConnection.commit()
        nLookIntoCount += 1

        stNow = DateTime.today()
        diffDateTime = stNow - dtStartDateTime
        # nRandomSec = random.randint(1, 15)
        nRandomSec = diffDateTime.total_seconds()
        if (nLookIntoCount % 10) == 0:
            print(GetLogDef.lineno(), DateTime.today().strftime('%H:%M:%S'), "Sleep! " + str(nRandomSec) + " Sec!")
            time.sleep(1)


        print(GetLogDef.lineno(__file__))


except Exception as e:
    print("Error Exception")
    print(e)
    print(type(e))

    # Switch 오류 업데이트
    dictSeoulSwitch = {}
    dictSeoulSwitch['seq'] = nSequence
    dictSeoulSwitch['state'] = 20
    print(GetLogDef.lineno(__file__), "dictSeoulSwitch >", dictSeoulSwitch)

else:
    print("========================= SUCCESS END")

finally:


    print('nTotalCount => ', nTotalCount)
    print('nTradeCount => ', nTradeCount)
    print('nRentCount => ', nRentCount)
    print('nLookIntoCount => ', nLookIntoCount)
    print('nNoLookCount => ', nNoLookCount)


    print("Finally END")


