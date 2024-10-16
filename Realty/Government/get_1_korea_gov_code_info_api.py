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




def main():
    try:
        #사용변수 초기화
        nSequence = 0

        print(GetLogDef.lineno(__file__), "============================================================")
        print(GetLogDef.lineno(__file__), "법정동 API 수집 프로그램")

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

        logFileName = str(dtNow.year) + str(dtNow.month) + str(dtNow.day).zfill(2) + ".log"

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(u'%(asctime)s [%(levelname)8s] %(message)s')

        streamingHandler = logging.StreamHandler()
        streamingHandler.setFormatter(formatter)

        # RotatingFileHandler
        log_max_size = 10 * 1024 * 1024  ## 10MB
        log_file_count = 20

        # RotatingFileHandler
        timeFileHandler = logging.handlers.TimedRotatingFileHandler(
            filename='D:/PythonProjects/airstock/Shell/logs/'+strProcessType+ '_get_bjdong_api' + logFileName,
            when='midnight',
            interval=1,
            encoding='utf-8'
        )
        timeFileHandler.setFormatter(formatter)
        logger.addHandler(streamingHandler)
        logger.addHandler(timeFileHandler)



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

        # print(GetLogDef.lineno(__file__), "[START LOOP]]================== ", nLoop)
        #
        # nbaseDate = dtToday - relativedelta(months=nLoop)
        dtProcessDay = str(int(dtNow.strftime("%Y%m")))

        print(GetLogDef.lineno(__file__), "dtProcessDay >> ", dtProcessDay)

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
        json_str = response.read().decode("utf-8")

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "json_str===> ", type(json_str),
              json_str)

        if len(json_str) < 1:
            Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), 'json_str => ', json_str)  # 예외를 발생시킴


        json_object = json.loads(json_str)

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), "json_str=> ", len(json_str), type(json_str), json_str)

        bMore = json_object.get('StanReginCd')
        if bMore is None:
            Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                inspect.getframeinfo(inspect.currentframe()).lineno), 'bMore => ', bMore)  # 예외를 발생시킴

        print("bMore > ", bMore)
        jsonResultHead = bMore[0].get('head')

        jsonResultDatasResult = bMore[1]

        # jsonResultDatasResult = jsonResultDatas
        # strResultCode = jsonResultDatasResult.get('CODE')
        # print("jsonResultHeads > ", jsonResultHead[0].get('totalCount'))
        nTotalCount = int(jsonResultHead[0].get('totalCount'))

        print("jsonResultDatasResult > ", jsonResultDatasResult)

        intMaxPage = math.ceil(nTotalCount / nProcessedCount)

        # intMaxPage = 1

        print("intMaxPage > ", intMaxPage)

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        while True:

            # 시작번호가 총 카운트 보다 많으면 중단
            if (intMaxPage > 0) and (nStartNumber > intMaxPage):
                break



            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            url  = "http://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList?serviceKey="+init_conf.MolitEncodedAuthorizationKey
            url += "&pageNo="+str(nStartNumber)+"&numOfRows="+str(nProcessedCount)+"&type=json"

            print(GetLogDef.lineno(__file__), "url > ", url)

            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            response = urllib.request.urlopen(url)
            json_str = response.read().decode("utf-8")

            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                                    inspect.getframeinfo(inspect.currentframe()).lineno), "url===> ", dtProcessDay, url)
            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                                    inspect.getframeinfo(inspect.currentframe()).lineno), "url===> ", dtProcessDay, url)
            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                                    inspect.getframeinfo(inspect.currentframe()).lineno), "response===> ", type(response), response)
            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                                    inspect.getframeinfo(inspect.currentframe()).lineno), "json_str===> ", type(json_str),
            #       json_str)
            #

            json_object = json.loads(json_str)
            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                                    inspect.getframeinfo(inspect.currentframe()).lineno), "json_str===> ", type(json_object),
            #       json_object)
            jsonResultDatas = json_object.get('StanReginCd')[1]

            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                                    inspect.getframeinfo(inspect.currentframe()).lineno), "jsonResultDatas===> ", type(jsonResultDatas),
            #       jsonResultDatas)

            jsonRowDatas = jsonResultDatas.get('row')

            # print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
            #                                    inspect.getframeinfo(inspect.currentframe()).lineno), "jsonRowDatas===> ", type(jsonRowDatas),
            #       jsonRowDatas)

            # 3. 건별 처리
            print("Processing", "====================================================")
            nLoop = 0
            strUniqueKey = ''

            for jsonRowData in jsonRowDatas:
                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                        inspect.getframeinfo(inspect.currentframe()).lineno), "jsonRowData===> ",
                      type(jsonRowData), jsonRowData)

                strUrlRegionCd = jsonRowData.get('region_cd')
                strUrlSidoCd = jsonRowData.get('sido_cd')
                strUrlSggCd = jsonRowData.get('sgg_cd')
                strUrlUmdCd = jsonRowData.get('umd_cd')
                strUrlRiCd = jsonRowData.get('ri_cd')

                strUrlLocatjuminCd = jsonRowData.get('locatjumin_cd')
                strUrlLocatjijukCd = jsonRowData.get('locatjijuk_cd')
                strUrlLocataddNm = jsonRowData.get('locatadd_nm')
                strUrlLocatOrder = jsonRowData.get('locat_order')
                strUrlLocatRm = jsonRowData.get('locat_rm')


                strUrlLocathighCd = jsonRowData.get('locathigh_cd')
                strUrlLocallowNm = jsonRowData.get('locallow_nm')
                strUrlAdptDe = jsonRowData.get('adpt_de')

                print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                        inspect.getframeinfo(inspect.currentframe()).lineno), "strUrlRegionCd===> ",
                      type(strUrlRegionCd), strUrlRegionCd)

                nLoop += 1


                sqlSelectGovCode = " SELECT * FROM " + ConstRealEstateTable.GovAddressAPIInfoTable
                sqlSelectGovCode += " WHERE region_cd = %s  "
                cursorRealEstate.execute(sqlSelectGovCode, (strUrlRegionCd))
                row_result = cursorRealEstate.rowcount
                # 등록되어 있는 물건이면 패스


                #이미 저장 되어 있으면 더이상 저장 하지 않는다.
                if row_result < 1:

                    # INSERT
                    sqlInsertGovCode = " INSERT INTO " + ConstRealEstateTable.GovAddressAPIInfoTable + " SET "
                    sqlInsertGovCode += " region_cd = %s ,  "
                    sqlInsertGovCode += " sido_cd = %s ,  "
                    sqlInsertGovCode += " sgg_cd = %s ,  "
                    sqlInsertGovCode += " umd_cd = %s ,  "
                    sqlInsertGovCode += " ri_cd = %s ,  "
                    sqlInsertGovCode += " locatjumin_cd = %s ,  "
                    sqlInsertGovCode += " locatjijuk_cd = %s ,  "
                    sqlInsertGovCode += " locatadd_nm = %s ,  "
                    sqlInsertGovCode += " locat_order = %s ,  "
                    sqlInsertGovCode += " locat_rm = %s ,  "
                    sqlInsertGovCode += " locathigh_cd = %s ,  "
                    sqlInsertGovCode += " locallow_nm = %s ,  "
                    sqlInsertGovCode += " adpt_de = %s ,  "
                    sqlInsertGovCode += " modify_date = NOW() ,  "
                    sqlInsertGovCode += " reg_date = NOW()   "
                    cursorRealEstate.execute(sqlInsertGovCode, (strUrlRegionCd, strUrlSidoCd, strUrlSggCd, strUrlUmdCd, strUrlRiCd,
                                                                strUrlLocatjuminCd, strUrlLocatjijukCd,strUrlLocataddNm, strUrlLocatOrder,strUrlLocatRm,
                                                                strUrlLocathighCd,strUrlLocallowNm,strUrlAdptDe))

                    print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno), "INSERT => ", strUrlRegionCd, strUrlSidoCd, strUrlSggCd, strUrlUmdCd, strUrlRiCd, strUrlLocatjuminCd, strUrlLocatjijukCd,strUrlLocataddNm, strUrlLocatOrder,strUrlLocatRm,strUrlLocathighCd,strUrlLocallowNm,strUrlAdptDe)

                ResRealEstateConnection.commit()

            nStartNumber += 1
            time.sleep(2)




        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = jsonIssueNumber
        dictSwitchData['data_2'] = jsonIssueNumber
        dictSwitchData['data_3'] = jsonIssueNumber
        dictSwitchData['data_4'] = jsonIssueNumber
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)
        print(GetLogDef.lineno(__file__), "[END strAdminName]]================== ", jsonIssueNumber)


    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'

        if KuIndex is not None:
            dictSwitchData['data_1'] = KuIndex

        if CityKey is not None:
            dictSwitchData['data_2'] = CityKey

        if targetRow is not None:
            dictSwitchData['data_3'] = targetRow

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        err_msg = traceback.format_exc()

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "Error Exception")
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     str(e))
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     str(err_msg))

    else:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[SUCCESS END]==================================================================")

    finally:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                     "[CRONTAB END]==================================================================")



