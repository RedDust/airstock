#서울시 공동주택 아파트 정보
#https://data.seoul.go.kr/dataList/OA-15818/S/1/datasetView.do


#http://openapi.seoul.go.kr:8088/(인증키)/xml/OpenAptInfo/1/5/

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


from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
import math
from Lib.GeoDataModule import GeoDataModule

import logging
import logging.handlers
import inspect
import traceback

def main():

    try:
        #사용변수 초기화
        nSequence = 0

        #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
        #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
        #거래 신고 30일 + 취소 신고 +30일
        stToday = DateTime.today()

        #서울 부동산 실거래가 데이터 - 임대차
        strProcessType = '034160'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'
        nbaseDate = stToday - TimeDelta(days=5)
        dtProcessDay = int(nbaseDate.strftime("%Y%m%d"))

        strProcessDay = str(dtProcessDay)
        strProcessYear = strProcessDay[0:4]

        # 한번에 처리할 건수
        nProcessedCount = 1000


        nTotalCount = 0

        # 실제[]
        nLoopTotalCount = 0

        # 시작번호
        nStartNumber = 1

        # 최종번호
        nEndNumber = nProcessedCount

        logFileName = str(stToday.year) + str(stToday.month).zfill(2)  + str(stToday.day).zfill(2) + ".log"



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
            filename='D:/PythonProjects/airstock/Shell/logs/' + strProcessType + '/' + logFileName,
            when='midnight',
            interval=1,
            encoding='utf-8'
        )
        timeFileHandler.setFormatter(formatter)
        logger.addHandler(streamingHandler)
        logger.addHandler(timeFileHandler)


        nInsertedCount = 0
        nUpdateCount = 0

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')

        if strResult is False:
            quit(GetLogDef.lineno(__file__), 'strResult => ', strResult)  # 예외를 발생시킴

        # if strResult == '10':
        #     quit(GetLogDef.lineno(__file__), 'It is currently in operation. => ', strResult)  # 예외를 발생시킴

        # if strResult == '30':
        #     quit(GetLogDef.lineno(__file__), 'It is Operation Error => ', strResult)  # 예외를 발생시킴


        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = KuIndex
        dictSwitchData['data_2'] = arrCityPlace
        dictSwitchData['data_3'] = targetRow
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +  "dtProcessDay >" + str(dtProcessDay) + " strProcessYear > "+ str(strProcessYear))

        while True:

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " nStartNumber >" + str(nStartNumber))
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " nEndNumber >" + str(nEndNumber))

            # 시작번호가 총 카운트 보다 많으면 중단
            if (nTotalCount > 0) and (nStartNumber > nTotalCount):
                break


            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/OpenAptInfo/" + str(nStartNumber) + "/" + str(nEndNumber)
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " url>>>" + url)


            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            response = urllib.request.urlopen(url)
            json_str = response.read().decode("utf-8")

            # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
            json_object = json.loads(json_str)
            bMore = json_object.get('OpenAptInfo')


            if bMore is None:
                raise Exception(GetLogDef.lineno(__file__)+ 'bMore => '+ str(bMore))  # 예외를 발생시킴
                break

            nTotalCount = bMore.get('list_total_count')

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + "nTotalCount > " + str(nTotalCount))

            jsonResultDatas = bMore.get('RESULT')
            jsonResultDatasResult = jsonResultDatas

            strResultCode = jsonResultDatasResult.get('CODE')
            strResultMessage = jsonResultDatasResult.get('MESSAGE')

            if strResultCode != 'INFO-000':
                raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + 'strResultCode => '+ str(strResultCode))  # 예외를 발생시킴
                break
                # GetOut while True:

            jsonRowDatas = bMore.get('row')

            # print(jsonRowDatas)

            SeoulSN = 1

            # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

            dictSeoulColumnInfoData = {}

            #필드 테이블 컬럼 타입 조회 int 인경우는 예외처리 하기 위한 준비
            qrySelectSeoulTableColumns = "show columns from " + ConstRealEstateTable_GOV.SeoulAPTInfoTable
            cursorRealEstate.execute(qrySelectSeoulTableColumns)
            SelectColumnLists = cursorRealEstate.fetchall()

            for SelectColumnList in SelectColumnLists:
                strFieldName = str(SelectColumnList.get('Field'))
                strColumnType = str(SelectColumnList.get('Type'))
                dictSeoulColumnInfoData[strFieldName] = {}
                dictSeoulColumnInfoData[strFieldName]['name'] = strFieldName
                dictSeoulColumnInfoData[strFieldName]['type'] = strColumnType

            nLoop = 0
            for list in jsonRowDatas:
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + "---------------------------------------------------------------------------------------------")
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +"[ "+str(nStartNumber)+" - "+str(nEndNumber)+" ][ "+str(nLoop)+" ] ")
                nLoop += 1

                dictSeoulRealtyTradeDataMaster = {}

                for dictSeoulMasterKeys in list.keys():

                    for dictNaverMobileMasterKeys in list.keys():
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = list.get(dictSeoulMasterKeys)

                    # Non-strings are converted to strings.
                    if type(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys]) is not str:
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = str(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys])

                #for dictSeoulMasterKeys in list.keys():

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +"dictSeoulRealtyTradeDataMaster > " +  dictSeoulRealtyTradeDataMaster['SN'])

                SeoulSN = math.floor(float(dictSeoulRealtyTradeDataMaster['SN']))
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +"SeoulSN > " + str(SeoulSN))

                APT_CODE = dictSeoulRealtyTradeDataMaster['APT_CODE']
                APT_NM = dictSeoulRealtyTradeDataMaster['APT_NM']

                qrySelectSeoulSwitch = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulAPTInfoTable + " WHERE SN=%s Limit 1 "
                cursorRealEstate.execute(qrySelectSeoulSwitch, str(SeoulSN))
                nResultCount = cursorRealEstate.rowcount
                SwitchDataList = cursorRealEstate.fetchone()

                #Log
                # logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                #                                inspect.getframeinfo(inspect.currentframe()).lineno) + "SwitchDataList> " + str())

                # strDBX_CODE = str(SwitchDataList.get('X_CODE'))
                # boolCheck = False
                # if len(strDBX_CODE) < 1:
                #     logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                #                                        inspect.getframeinfo(inspect.currentframe()).lineno) + "dictSeoulRealtyTradeDataMaster[SN] > " + str(dictSeoulRealtyTradeDataMaster['SN']))
                #     boolCheck = True

                if nResultCount > 0: #UPDATE
                    nSequence   = str(SwitchDataList.get('seq'))
                    strStateCode = str(SwitchDataList.get('state'))

                    strDBX_CODE = str(SwitchDataList.get('X_CODE'))
                    strDBY_CODE = str(SwitchDataList.get('Y_CODE'))

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno) +  " > strDBX_CODE " + strDBX_CODE)
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                            inspect.getframeinfo(inspect.currentframe()).lineno) +  " > strDBY_CODE" + strDBY_CODE)

                    if len(strDBX_CODE) < 4 or len(strDBY_CODE) < 3:
                        strDBADRES_DORO = str(SwitchDataList.get('ADRES_DORO'))
                        strDBADRES_RMNDR = str(SwitchDataList.get('ADRES_RMNDR'))

                        strDOROJUSO  = str(SwitchDataList.get('ADRES_CITY')) + " "
                        strDOROJUSO += str(SwitchDataList.get('ADRES_GU')) + " "
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " > " + strDBADRES_DORO)
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename, inspect.getframeinfo(inspect.currentframe()).lineno) + " > strDOROJUSO[ADRES_CITY + ADRES_GU ]" + strDOROJUSO)

                        if len(strDBADRES_DORO) > 2:
                            strDOROJUSO += str(SwitchDataList.get('ADRES_DORO')) + " " + str(SwitchDataList.get('ADRES_DORO_RMNDR'))
                            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " > strDOROJUSO[ADRES_DORO + ADRES_DORO_RMNDR ]" + strDOROJUSO)
                        else:
                            strDOROJUSO += str(SwitchDataList.get('ADRES_DONG')) + " "
                            strDOROJUSO += str(SwitchDataList.get('APT_NM'))
                            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " > strDOROJUSO[ADRES_DONG + APT_NM ]" + strDOROJUSO)


                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " > strDOROJUSO" + strDOROJUSO)


                        resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
                        if isinstance(resultsDict, dict) == False:
                            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +   "strDOROJUSO > " + strDOROJUSO)

                            strDOROJUSO = str(SwitchDataList.get('DOROJUSO')) + " "
                            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + "strDOROJUSO[DOROJUSO] >" + strDOROJUSO)

                            resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
                            if isinstance(resultsDict, dict) == False:
                                raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +  'strDOROJUSO => ' + str(strDOROJUSO))  # 예외를 발생시킴

                            else:
                                strDBLongitude = resultsDict['x']
                                strDBLatitude = resultsDict['y']

                        else:
                            strDBLongitude = resultsDict['x']
                            strDBLatitude = resultsDict['y']

                    else:
                        strDBLongitude = str(SwitchDataList.get('naver_longitude'))
                        strDBLatitude = str(SwitchDataList.get('naver_latitude'))

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " strDBLongitude >" + strDBLongitude )
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " strDBLatitude >" + strDBLatitude)

                    # UPDATE 쿼리 작성
                    qryInfoUpdate  = " UPDATE " + ConstRealEstateTable_GOV.SeoulAPTInfoTable + " SET "
                    qryInfoUpdate += " modify_date=now()"

                    if len(strDBLongitude) < 4 or len(strDBLatitude) < 3:

                        resultsDict = GeoDataModule.getNaverReverseGeoData(logger, strDBX_CODE , strDBY_CODE)
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename, inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[resultsDict] => " + str(resultsDict))

                        strDOROJUSO  = resultsDict['dosi_name'] + " "
                        strDOROJUSO += resultsDict['sigu_name'] + " "
                        strDOROJUSO += resultsDict['dong_name'] + " "
                        strDOROJUSO += resultsDict['bone_bun']
                        if len(resultsDict['bu_bun']) > 0:
                            strDOROJUSO += "-" + resultsDict['bu_bun']

                        strConversionKeyWord = str(GetLogDef.stripSpecharsForText(strDOROJUSO)).strip()

                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strConversionKeyWord] => " + str(strConversionKeyWord))

                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +
                                     "[strDOROJUSO] => " + str(strDOROJUSO))

                        resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
                        if isinstance(resultsDict, dict) == False:
                            raise Exception(GetLogDef.lineno(__file__) + 'resultsDict => ' + str(resultsDict))  # 예외를 발생시킴

                        strNaverLongitude = resultsDict['x']
                        strNaverLatitude = resultsDict['y']
                    else:

                        strNaverLongitude = strDBLongitude
                        strNaverLatitude = strDBLatitude

                    qryInfoUpdate += " , naver_longitude = '"+strNaverLongitude+"' "
                    qryInfoUpdate += " , naver_latitude = '" + strNaverLatitude + "' "
                    qryInfoUpdate += " , geo_point = ST_GeomFromText('POINT(" + strNaverLongitude +" "+ strNaverLatitude +")', 4326, 'axis-order=long-lat') "

                    qryInfoUpdate += " WHERE seq='"+str(nSequence)+"' "
                    cursorRealEstate.execute(qryInfoUpdate)
                    ResRealEstateConnection.commit()

                else: #INSERT
                    #INSERT
                    qryInfoInsert = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulAPTInfoTable +" SET "
                    qryInfoInsert += " modify_date=NOW()"
                    for dictSeoulAptInfoDataMaster in dictSeoulRealtyTradeDataMaster.keys():

                        # print("dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster]", dictSeoulAptInfoDataMaster, dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster], type(dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster]))
                        # fields 타입이 int 면 empty 인 경우 예외 처리 - 타입오류 예방
                        if dictSeoulColumnInfoData[dictSeoulAptInfoDataMaster]['type'] == 'int' and dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] =='':
                            dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] = '0'

                        if dictSeoulColumnInfoData[dictSeoulAptInfoDataMaster]['type'] == 'datetime' and dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] =='':
                            dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] = '0000-00-00 00:00:00'

                        qryInfoInsert += " ," + dictSeoulAptInfoDataMaster + "='" + dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] + "' "


                    strDBX_CODE = str(dictSeoulRealtyTradeDataMaster['X_CODE'])
                    strDBY_CODE = str(dictSeoulRealtyTradeDataMaster['Y_CODE'])

                    resultsDict = GeoDataModule.getNaverReverseGeoData(logger, strDBX_CODE, strDBY_CODE)
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[resultsDict] => " + str(resultsDict))

                    strDOROJUSO = resultsDict['dosi_name'] + " "
                    strDOROJUSO += resultsDict['sigu_name'] + " "
                    strDOROJUSO += resultsDict['dong_name'] + " "
                    strDOROJUSO += resultsDict['bone_bun']
                    if len(resultsDict['bu_bun']) > 0:
                        strDOROJUSO += "-" + resultsDict['bu_bun']

                    strConversionKeyWord = str(GetLogDef.stripSpecharsForText(strDOROJUSO)).strip()

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[strConversionKeyWord] => " + str(strConversionKeyWord))

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                                 "[strDOROJUSO] => " + str(strDOROJUSO))

                    resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
                    if isinstance(resultsDict, dict) == False:
                        raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +  'resultsDict => ' + str(resultsDict))  # 예외를 발생시킴

                    strNaverLongitude = resultsDict['x']
                    strNaverLatitude = resultsDict['y']

                    qryInfoInsert += " , naver_longitude = '"+strNaverLongitude+"' "
                    qryInfoInsert += " , naver_latitude = '" + strNaverLatitude + "' "
                    qryInfoInsert += " , geo_point = ST_GeomFromText('POINT(" + strNaverLongitude +" "+ strNaverLatitude +")', 4326, 'axis-order=long-lat') "
                    qryInfoInsert += ", reg_date=NOW()"

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + " [qryInfoInsert >> " + qryInfoInsert)
                    cursorRealEstate.execute(qryInfoInsert)
                    ResRealEstateConnection.commit()
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + "qryInfoInsert ResRealEstateConnection.commit()")

            # for list in jsonRowDatas:

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + "dictSeoulRealtyTradeDataMaster")
            nStartNumber = nEndNumber + 1
            nEndNumber = nEndNumber + nProcessedCount
        #while True:

    except Exception as e:

        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +  "Error Exception")
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) + e)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        if dtProcessDay is not None:
            dictSwitchData['data_1'] = dtProcessDay

        if strProcessDay is not None:
            dictSwitchData['data_2'] = strProcessDay

        if SeoulSN is not None:
            dictSwitchData['data_3'] = SeoulSN

        if APT_CODE is not None:
            dictSwitchData['data_4'] = APT_CODE

        if APT_NM is not None:
            dictSwitchData['data_5'] = APT_NM

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno) +  "SUCCESS")
        # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['data_1'] = dtProcessDay
        dictSwitchData['data_2'] = strProcessDay
        dictSwitchData['data_3'] = nStartNumber
        dictSwitchData['data_4'] = nEndNumber
        dictSwitchData['data_5'] = nSequence
        dictSwitchData['data_6'] = nInsertedCount
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    finally:
        logging.info(GetLogDef.lineno(__file__) + "Finally END")
        ResRealEstateConnection.close()
