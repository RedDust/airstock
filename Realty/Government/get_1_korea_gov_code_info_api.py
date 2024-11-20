# 법정동 API 수집 프로그램
#법정동 API
#https://www.data.go.kr/iim/api/selectAPIAcountView.do

#END POINT
#http://apis.data.go.kr/1741000/StanReginCd
#http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList


import sys
sys.path.append("D:/PythonProjects/airstock")


import requests

# This is a sample Python script.
import sys
import json
import random
import pymysql
import logging
import logging.handlers
import inspect
import traceback
import re
import urllib.request



import datetime, time, inspect, math

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from Init.DefConstant import ConstRealEstateTable

from Lib.RDB import pyMysqlConnector

from Init.Functions.Logs import GetLogDef


from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
from Lib.CustomException import QuitException

import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF


def main():

    try:
        #사용변수 초기화
        nSequence = 0

        strProcessType = '042101'
        KuIndex = DBKuIndex = '00'
        CityKey = DBCityKey = '00'
        targetRow = '00'
        strAuctionUniqueNumber = '00'
        strAuctionSeq = '0'
        jsonIssueNumber = '0'
        nInsertedCount = 0
        nUpdateCount = 0
        dtNow = DateTime.today()
        # print(dtNow.hour)
        # print(dtNow.minute)
        # print(dtNow)

        LogPath = 'Common/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()),
                     "============================================================")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            QuitException.QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + '[strResult =>'+ strResult+"]" )# 예외를 발생시킴

        if strResult == '10':
            QuitException.QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + '[It is currently in operation. => =>' + strResult + "]")  # 예외를 발생시킴


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

        # print(GetLogDef.lineno(__file__), "[START LOOP]]================== ", nLoop)
        #
        # nbaseDate = dtToday - relativedelta(months=nLoop)
        dtProcessDay = str(int(dtNow.strftime("%Y%m")))
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[dtProcessDay >>" + dtProcessDay + "]")
        # 한번에 처리할 건수
        nProcessedCount = 1000

        nTotalCount = 0

        # 실제[]
        nLoopTotalCount = 0

        # 페이지 번호
        nStartNumber = 1

        # 최종번호
        nEndNumber = nProcessedCount


        # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
        url = "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList?serviceKey=" + init_conf.MolitEncodedAuthorizationKey
        url += "&pageNo=" + str(nStartNumber) + "&numOfRows=1&type=json"

        # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
        response = urllib.request.urlopen(url)


        if response.getcode() != 200:
            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            url = "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList?serviceKey=" + init_conf.MolitEncodedAuthorizationKey
            url += "&pageNo=" + str(nStartNumber) + "&numOfRows=1&type=json"
            response = urllib.request.urlopen(url)

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[response >>" + str(response) + "]")
        json_str = response.read().decode("utf-8")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[json_str >>" + str(json_str) + "]")

        if len(json_str) < 1:
            Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[json_str >>" + str(json_str) + "]")  # 예외를 발생시킴


        json_object = json.loads(json_str)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[json_object >>" + str(json_object) + "]")

        bMore = json_object.get('StanReginCd')
        if bMore is None:
            Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[bMore >>" + str(bMore) + "]")  # 예외를 발생시킴

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[bMore >>" + str(bMore) + "]")
        jsonResultHead = bMore[0].get('head')

        jsonResultDatasResult = bMore[1]

        # jsonResultDatasResult = jsonResultDatas
        # strResultCode = jsonResultDatasResult.get('CODE')
        # print("jsonResultHeads > ", jsonResultHead[0].get('totalCount'))
        nTotalCount = int(jsonResultHead[0].get('totalCount'))

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[jsonResultDatasResult >>" + str(jsonResultDatasResult) + "]")
        intMaxPage = math.ceil(nTotalCount / nProcessedCount)

        # intMaxPage = 1

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[intMaxPage >>" + str(intMaxPage) + "]")
        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        #수집전에 테이블을 모두 사용 불가능으로 바꿔 놓는다.
        # 없어진 시군구 코드를 체크 하기 위해서

        sqlUpdateGovCode = " UPDATE " + ConstRealEstateTable.GovAddressAPIInfoTable + " SET "
        sqlUpdateGovCode += " state = '01'  "
        cursorRealEstate.execute(sqlUpdateGovCode)
        row_result = cursorRealEstate.rowcount
        if row_result < 1:
            Exception(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[row_result >>" + str(row_result) + "]")  # 예외를 발생시킴


        while True:

            # 시작번호가 총 카운트 보다 많으면 중단
            if (intMaxPage > 0) and (nStartNumber > intMaxPage):
                break

            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            url  = "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList?serviceKey="+init_conf.MolitEncodedAuthorizationKey
            url += "&pageNo="+str(nStartNumber)+"&numOfRows="+str(nProcessedCount)+"&type=json"

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[url >>" + str(url) + "]")

            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            while True:
                response = urllib.request.urlopen(url)
                if response.getcode() == 200:
                    # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
                    break

                time.sleep(1)

            json_str = response.read().decode("utf-8")
            json_object = json.loads(json_str)
            jsonResultDatas = json_object.get('StanReginCd')[1]
            jsonRowDatas = jsonResultDatas.get('row')
            # 3. 건별 처리
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()),
                         "Processing============================================================")

            nLoop = 0
            strUniqueKey = ''

            for jsonRowData in jsonRowDatas:

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "nLoop============================================================")
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[jsonRowData >>" + str(jsonRowData) + "]")


                strUrlRegionCd = jsonRowData.get('region_cd')
                strUrlSidoCd = jsonRowData.get('sido_cd')
                strUrlSggCd = jsonRowData.get('sgg_cd')
                strUrlUmdCd = jsonRowData.get('umd_cd')
                strUrlRiCd = jsonRowData.get('ri_cd')

                strSiguName = str(jsonRowData.get('locatadd_nm'))
                listSiGuName = strSiguName.split(" ")
                listSiGuNameTemp = strSiguName.split(" ")

                strSiGuName = strUmdName = strRiName = ''
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[listSiGuName >>" + str(listSiGuName) + "]")

                strSiDoName = str(listSiGuName[0])
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strSiDoName >>" + str(strSiDoName) + "]")

                listSiGuNameTemp.pop(0)

                if strUrlSggCd != "000" and len(listSiGuName) > 1:
                    strSiGuName = str(listSiGuName[1])
                    strLastPop = listSiGuNameTemp.pop(0)
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strLastPop >>" + str(strLastPop) + "]")

                    strLastWord = strLastPop[-1]
                    if strLastWord.find('시') < 0 and strLastWord.find('군') < 0 and strLastWord.find('구') < 0 :
                        listSiGuNameTemp.insert(0, strLastPop )
                        strSiGuName = ''

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strSiGuName >>" + str(strSiGuName) + "]")


                if strUrlRiCd != "00" and len(listSiGuName) > 2:
                    strRiName = listSiGuNameTemp.pop()


                if strUrlUmdCd != "000" and len(listSiGuName) > 1:
                    strUmdName = str(" ".join(listSiGuNameTemp))


                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUmdName >>" + str(strUmdName) + "]")

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strRiName >>" + str(strRiName) + "]")


                strUrlLocatjuminCd = jsonRowData.get('locatjumin_cd')
                strUrlLocatjijukCd = jsonRowData.get('locatjijuk_cd')
                strUrlLocataddNm = jsonRowData.get('locatadd_nm')
                strUrlLocatOrder = jsonRowData.get('locat_order')
                strUrlLocatRm = jsonRowData.get('locat_rm')


                strUrlLocathighCd = jsonRowData.get('locathigh_cd')
                strUrlLocallowNm = jsonRowData.get('locallow_nm')
                strUrlAdptDe = jsonRowData.get('adpt_de')

                logging.info(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlRegionCd >>" + str(strUrlRegionCd) + "]")

                nLoop += 1


                sqlSelectGovCode = " SELECT * FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
                sqlSelectGovCode += " WHERE region_cd = %s  "
                cursorRealEstate.execute(sqlSelectGovCode, (strUrlRegionCd))
                row_result = cursorRealEstate.rowcount
                # 등록되어 있는 물건이면 패스


                #이미 저장 되어 있으면 더이상 저장 하지 않는다.
                if row_result < 1:

                    strDOROJUSO = strSiguName

                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strDOROJUSO >>" + str(
                            strDOROJUSO) + "]")


                    resultsDict = GeoDataModule.getJusoData(strDOROJUSO)
                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[resultsDict >>" + str(
                            resultsDict) + "]")

                    if isinstance(resultsDict, dict) == True:
                        logging.info(
                            SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[resultsDict['jibunAddr'] >>" + str(
                                resultsDict['jibunAddr']) + "]")

                        strDOROJUSO = str(resultsDict['roadAddrPart1']).strip()

                    resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)

                    logging.info(
                        SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[resultsDict['resultsDict'] >>" + str(
                            resultsDict) + "]")



                    if isinstance(resultsDict, dict) != False:
                        strNaverLongitude = str(resultsDict['x'])  # 127
                        strNaverLatitude = str(resultsDict['y'])  # 37

                    time.sleep(0.3)



                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlRegionCd >>" + str(strUrlRegionCd) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlSidoCd >>" + str(
                        strUrlSidoCd) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strSiDoName >>" + str(
                        strSiDoName) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlSggCd >>" + str(
                        strUrlSggCd) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strSiGuName >>" + str(
                        strSiGuName) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlUmdCd >>" + str(
                        strUrlUmdCd) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUmdName >>" + str(
                        strUmdName) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlRiCd >>" + str(
                        strUrlRiCd) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strRiName >>" + str(
                        strRiName) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlLocatjuminCd >>" + str(
                        strUrlLocatjuminCd) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlLocatjijukCd >>" + str(
                        strUrlLocatjijukCd) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlLocataddNm >>" + str(
                        strUrlLocataddNm) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlLocatOrder >>" + str(
                        strUrlLocatOrder) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlLocatRm >>" + str(
                        strUrlLocatRm) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlLocathighCd >>" + str(
                        strUrlLocathighCd) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlLocallowNm >>" + str(
                        strUrlLocallowNm) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlAdptDe >>" + str(
                        strUrlAdptDe) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strNaverLongitude >>" + str(
                        strNaverLongitude) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strNaverLatitude >>" + str(
                        strNaverLatitude) + "]")



                    # INSERT
                    sqlInsertGovCode = " INSERT INTO " + ConstRealEstateTable.GovAddressAPIInfoTable + " SET "
                    sqlInsertGovCode += " region_cd = %s ,  "
                    sqlInsertGovCode += " sido_cd = %s ,  "
                    sqlInsertGovCode += " sido_nm = %s ,  "
                    sqlInsertGovCode += " sgg_cd = %s ,  "
                    sqlInsertGovCode += " sgg_nm = %s ,  "
                    sqlInsertGovCode += " umd_cd = %s ,  "
                    sqlInsertGovCode += " umd_nm = %s ,  "
                    sqlInsertGovCode += " ri_cd = %s ,  "
                    sqlInsertGovCode += " ri_nm = %s ,  "
                    sqlInsertGovCode += " locatjumin_cd = %s ,  "
                    sqlInsertGovCode += " locatjijuk_cd = %s ,  "
                    sqlInsertGovCode += " locatadd_nm = %s ,  "
                    sqlInsertGovCode += " locat_order = %s ,  "
                    sqlInsertGovCode += " locat_rm = %s ,  "
                    sqlInsertGovCode += " locathigh_cd = %s ,  "
                    sqlInsertGovCode += " locallow_nm = %s ,  "
                    sqlInsertGovCode += " adpt_de = %s ,  "
                    sqlInsertGovCode += " nlongitude = %s ,  "
                    sqlInsertGovCode += " nlatitude = %s ,  "
                    sqlInsertGovCode += " geo_point = ST_GeomFromText('POINT(" + strNaverLongitude + " " + strNaverLatitude + ")', 4326,'axis-order=long-lat'), "
                    sqlInsertGovCode += " modify_date = NOW() ,  "
                    sqlInsertGovCode += " reg_date = NOW()   "


                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[sqlInsertGovCode >>" + str(
                        sqlInsertGovCode) + "]")

                    cursorRealEstate.execute(sqlInsertGovCode, (strUrlRegionCd, strUrlSidoCd,strSiDoName, strUrlSggCd, strSiGuName, strUrlUmdCd, strUmdName, strUrlRiCd, strRiName,
                                                                strUrlLocatjuminCd, strUrlLocatjijukCd,strUrlLocataddNm, strUrlLocatOrder,strUrlLocatRm,
                                                                strUrlLocathighCd,strUrlLocallowNm,strUrlAdptDe, strNaverLongitude, strNaverLatitude))



                else:
                    # UPDATE
                    sqlUpdateGovCode = " UPDATE " + ConstRealEstateTable.GovAddressAPIInfoTable + " SET "
                    sqlUpdateGovCode += " state = '00'  "
                    sqlUpdateGovCode += " , modify_date = NOW()  "
                    sqlUpdateGovCode += " WHERE region_cd = %s  "

                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[sqlUpdateGovCode >>" + str(
                        sqlUpdateGovCode) + "]")
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[strUrlRegionCd >>" + str(
                        strUrlRegionCd) + "]")

                    cursorRealEstate.execute(sqlUpdateGovCode, (strUrlRegionCd))
                    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[UPDATE >>" + str(
                        strUrlRegionCd) + "]")


                ResRealEstateConnection.commit()

                # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
                dictSwitchData = dict()
                dictSwitchData['result'] = '10'
                dictSwitchData['data_1'] = strUrlRegionCd
                dictSwitchData['data_2'] = nStartNumber
                dictSwitchData['data_3'] = nProcessedCount
                dictSwitchData['data_4'] = strUrlRegionCd
                dictSwitchData['data_5'] = strUrlLocataddNm
                dictSwitchData['data_6'] = strUrlLocallowNm
                LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            nStartNumber += 1
            time.sleep(2)


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = strUrlRegionCd
        dictSwitchData['data_2'] = nStartNumber
        dictSwitchData['data_3'] = nProcessedCount
        dictSwitchData['data_4'] = strUrlRegionCd
        dictSwitchData['data_5'] = strUrlLocataddNm
        dictSwitchData['data_6'] = strUrlLocallowNm
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[END strAdminName]]================= >>" + str(
            dictSwitchData) + "]")


    except Exception as e:

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[Error Exception]]================= >>")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'

        if KuIndex is not None:
            dictSwitchData['data_1'] = strUrlRegionCd

        if CityKey is not None:
            dictSwitchData['data_2'] = nStartNumber

        if targetRow is not None:
            dictSwitchData['data_3'] = nProcessedCount


        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +"[dictSwitchData >> " + str(dictSwitchData) +"]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg >>" + str(err_msg) + "]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e >>" + str(e) + "]")

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[SUCCESS END]================= >>")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[CRONTAB END]================= >>")
