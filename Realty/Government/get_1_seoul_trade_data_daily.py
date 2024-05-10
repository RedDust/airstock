# 서울 일별 실거래가 데이터 수집 프로그램
# 2023-01-30 커밋
# 취소 데이터 때문에 2년전 데이터 까지 조회
# https://data.seoul.go.kr/dataList/OA-21275/S/1/datasetView.do
import sys
import urllib.request
import json
import pymysql
import datetime
import time
import pandas as pd
import logging , inspect

sys.path.append("D:/PythonProjects/airstock")
#https://data.seoul.go.kr/dataList/OA-21275/S/1/datasetView.do



from pandas.io.json import json_normalize
from Realty.Government.Init import init_conf
from Lib.RDB import pyMysqlConnector

from SeoulLib.RDB import LibSeoulRealTradeSwitch

from Init.Functions.Logs import GetLogDef

from Realty.Government.Const import ConstRealEstateTable_GOV

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule

try:

    #60일 이전이 같은 해 이면 해당 연 데이터만 취합하고
    #1월 2월 의 경우로 지난 해를 포함 하면 지난해 데이터도 취합한다.
    #거래 신고 30일 + 취소 신고 +30일
    stToday = DateTime.today()

    nInsertedCount = 0
    nUpdateCount = 0

    #서울 부동산 실거래가 데이터 - 매매
    strProcessType = '034100'
    KuIndex = '00'
    arrCityPlace = '00'
    targetRow = '00'

    dtNow = DateTime.today()

    dtNowYYYY = str(dtNow.year).zfill(4)
    dtNowMM = str(dtNow.month).zfill(2)
    dtNowDD = str(dtNow.day).zfill(2)

    dtNowHH = str(dtNow.hour).zfill(2)
    dtNowII = str(dtNow.minute).zfill(2)

    dtSearchYYYYMMDD = dtNowYYYY + dtNowMM + dtNowDD
    dtSearchHHII = dtNowHH + dtNowII

    logFileName = dtSearchYYYYMMDD + ".log"

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
        filename='D:/PythonProjects/airstock/Shell/logs/Cron_' + strProcessType + '_' + logFileName,
        when='midnight',
        interval=1,
        encoding='utf-8'
    )
    timeFileHandler.setFormatter(formatter)
    logger.addHandler(streamingHandler)
    logger.addHandler(timeFileHandler)
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +
                 "[CRONTAB START]==================================================================")


    # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
    rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
    strResult = rstResult.get('result')
    if strResult is False:
        quit(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + 'strResult => ' + strResult)  # 예외를 발생시킴

    if strResult == '10':
        quit(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + 'It is currently in operation. => ' + strResult)  # 예외를 발생시킴


    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '10'
    dictSwitchData['data_1'] = KuIndex
    dictSwitchData['data_2'] = arrCityPlace
    dictSwitchData['data_3'] = targetRow
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

    # intStartLoop = 0
    # intEndLoop = (365 * 4)

    intStartLoop = (365*4)
    intEndLoop = (365 * 5)

    intStartLoop = 0
    intEndLoop = (365 * 2)

    for nLoop in range(intStartLoop, intEndLoop):
    # for nLoop in range(0, 730):
        nbaseDate = stToday - TimeDelta(days=nLoop)
        dtProcessDay = int(nbaseDate.strftime("%Y%m%d"))

        # 한번에 처리할 건수
        nProcessedCount = 1000

        nTotalCount = 0

        # 실제[]
        nLoopTotalCount = 0

        # 시작번호
        nStartNumber = 1

        # 최종번호
        nEndNumber = nProcessedCount

        strProcessDay = str(dtProcessDay)
        strProcessYear = strProcessDay[0:4]
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "dtProcessDay >"+ str(dtProcessDay) + "strProcessYear > "+ str(strProcessYear) )

        while True:

            # 시작번호가 총 카운트 보다 많으면 중단
            if (nTotalCount > 0) and (nStartNumber > nTotalCount):
                break

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno)+ "nStartNumber >"+ str(nStartNumber))
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno)+ "nEndNumber >"+ str(nEndNumber))

            # # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            # url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/tbLnOpendataRtmsV/"+str(nStartNumber)+"/"+str(nEndNumber)+"/"+strProcessYear

            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/tbLnOpendataRtmsV/"+str(nStartNumber)+"/"+str(nEndNumber) \
                  + "/%20/%20/%20/%20/%20/%20/%20/%20/%20/" + strProcessDay + "/%20/"

            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno)+ "url > "+ str(url))

            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            response = urllib.request.urlopen(url)
            json_str = response.read().decode("utf-8")

            # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
            json_object = json.loads(json_str)
            bMore = json_object.get('tbLnOpendataRtmsV')

            if bMore is None:
                Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + 'bMore => '+ str(bMore))  # 예외를 발생시킴
                break

            jsonResultDatas = bMore.get('RESULT')
            jsonResultDatasResult = jsonResultDatas

            strResultCode = jsonResultDatasResult.get('CODE')
            strResultMessage = jsonResultDatasResult.get('MESSAGE')

            if strResultCode != 'INFO-000':
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno)+ strResultCode+ str(strResultMessage))
                break
                #GetOut while True:

            nTotalCount = bMore.get('list_total_count')
            jsonRowDatas = bMore.get('row')

            # # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


            # 3. 건별 처리
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "====================================================")

            nLoop = 0
            strUniqueKey = ''

            for list in jsonRowDatas:
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno)+ "------------------------------------------------------------------------------")
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno)+ "[ "+str(nStartNumber)+" - "+str(nEndNumber)+" ][ "+str(nLoop)+" ] ")
                nLoop += 1

                # DB 연결

                dictSeoulRealtyTradeDataMaster = {}

                for dictSeoulMasterKeys in list.keys():

                    for dictNaverMobileMasterKeys in list.keys():
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = list.get(dictSeoulMasterKeys)

                    # Non-strings are converted to strings.
                    if type(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys]) is not str:
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = str(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys])


                # `ACC_YEAR`, `SGG_CD`, `BJDONG_CD`,BONBEON BUBEON , `FLOOR` `DEAL_YMD`, `OBJ_AMT`
                strUniqueKey = dictSeoulRealtyTradeDataMaster['ACC_YEAR'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['SGG_CD'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['BJDONG_CD'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['BONBEON'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['BUBEON'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['FLOOR'] + "_" +\
                               dictSeoulRealtyTradeDataMaster['DEAL_YMD'] + "_" + dictSeoulRealtyTradeDataMaster['OBJ_AMT']

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno)+ "strUniqueKey > "+ str(strUniqueKey))

                cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
                qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + "  WHERE unique_key=%s"

                cursorRealEstate.execute(qrySelectSeoulTradeMaster, strUniqueKey)
                row_result = cursorRealEstate.rowcount
                # 등록되어 있는 물건이면 패스

                if len(dictSeoulRealtyTradeDataMaster['CNTL_YMD']) > 5:
                    strState = "10"
                else:
                    strState = "00"

                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno)+ "row_result => "+ str(row_result))

                if row_result > 0:

                    rstSelectDatas = cursorRealEstate.fetchone()
                    strCancelState = rstSelectDatas.get('state')

                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                   inspect.getframeinfo(
                                                       inspect.currentframe()).lineno) + "strCancelState => " + str(
                        strCancelState))

                    #10 이미 취소된 건은 UPDATE 하지 않아요.
                    if strCancelState == '10':
                        continue
                    # 조회 00 인 것은 업데이트 하지 않아요.(취소 되었다가 다시 복귀 되는 경우는 없어요.. - 신규 거래(INSERT)로 처리 해야 해요)
                    if strState == '00':
                        continue


                    # if rstSelectDatas.get('lng') == '' or rstSelectDatas.get('lat') == '':
                    #     floatTradeDBMasterLongitude = float(0)
                    #     floatTradeDBMasterLatitude = float(0)
                    # else:
                    #     floatTradeDBMasterLongitude = float(rstSelectDatas.get('lng'))
                    #     floatTradeDBMasterLatitude = float(rstSelectDatas.get('lat'))
                    #
                    #
                    # strDBHouseType = str(rstSelectDatas.get('HOUSE_TYPE'))
                    #
                    # logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno)+"floatTradeDBMasterLongitude => "+  str(floatTradeDBMasterLongitude ))
                    # logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno)+ "floatTradeDBMasterLatitude "+  str(floatTradeDBMasterLatitude ))


                    # strTradeDBMasterSGG_NM = str(rstSelectDatas.get('SGG_NM'))
                    # strTradeDBMasterBJDONG_NM = str(rstSelectDatas.get('BJDONG_NM'))
                    # strTradeDBMasterBONBEON = str(rstSelectDatas.get('BONBEON'))
                    # strTradeDBMasterBUBEON = str(rstSelectDatas.get('BUBEON'))
                    # strTradeDBBLDG_NM = str(rstSelectDatas.get('BLDG_NM'))
                    # strTradeDBLAND_GBN_NM = str(rstSelectDatas.get('LAND_GBN_NM'))
                    #
                    #
                    # strNaverLongitude = str(floatTradeDBMasterLongitude)  # 127
                    # strNaverLatitude = str(floatTradeDBMasterLatitude)  # 37
                    #
                    # logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) + "strNaverLongitude => "+ str(strNaverLongitude))
                    # logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) + "strNaverLatitude => " + str(strNaverLatitude))
                    # logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) + " strDBHouseType => "+ str(strDBHouseType))

                    # if strDBHouseType != str('단독다가구') and strTradeDBLAND_GBN_NM != '블럭':
                    #
                    #     if floatTradeDBMasterLongitude <= 10 or floatTradeDBMasterLatitude <= 10:
                    #
                    #         logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) + "floatTradeDBMasterLatitude => "+ str(floatTradeDBMasterLatitude))
                    #         logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) + "floatTradeDBMasterLatitude => "+ str(floatTradeDBMasterLatitude))
                    #
                    #         strTradeDBMasterBUBEON = str(strTradeDBMasterBUBEON).lstrip("0")
                    #
                    #         if strTradeDBMasterBUBEON != '':
                    #             strTradeDBMasterBUBEON = "-" + strTradeDBMasterBUBEON
                    #
                    #         strDOROJUSO = "서울특별시 "
                    #         strDOROJUSO += strTradeDBMasterSGG_NM + " "
                    #         strDOROJUSO += strTradeDBMasterBJDONG_NM + " "
                    #         strDOROJUSO += str(strTradeDBMasterBONBEON).lstrip("0")
                    #         strDOROJUSO += str(strTradeDBMasterBUBEON).lstrip("0")
                    #         logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) + "strDOROJUSO => " + str(strDOROJUSO))
                    #
                    #         resultsDict = GeoDataModule.getJusoData(strDOROJUSO)
                    #         logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) + "resultsDict >"+str(isinstance(resultsDict, dict)) +  str(resultsDict))
                    #         if isinstance(resultsDict, dict) == False:
                    #             logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) +  "resultsDict >" + str(resultsDict))
                    #
                    #         else:
                    #             logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) +  str(resultsDict['jibunAddr']))
                    #             strDOROJUSO = str(resultsDict['roadAddrPart1']).strip()
                    #
                    #         resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
                    #         logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) + "resultsDict >" +  str(resultsDict))
                    #
                    #         if isinstance(resultsDict, dict) == False:
                    #             strNaverLongitude = str(0)
                    #             strNaverLatitude = str(0)
                    #
                    #         else:
                    #             strNaverLongitude = str(resultsDict['x'])  # 127
                    #             strNaverLatitude = str(resultsDict['y'])  # 37
                    #
                    #     else:
                    #         logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) + "if(len(strCNTLYMD) < 1 ) END" + str(strNaverLongitude))
                    #         logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                    #                inspect.getframeinfo(inspect.currentframe()).lineno) +  "if(len(strCNTLYMD) < 1 ) END" + str(strNaverLatitude))

                    strCNTLYMD = rstSelectDatas.get('CNTL_YMD')

                    sqlSeoulRealTrade  = " UPDATE " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + " SET "
                    sqlSeoulRealTrade += " CNTL_YMD='" + dictSeoulRealtyTradeDataMaster['CNTL_YMD'] + "' "
                    sqlSeoulRealTrade += " , state='" + strState + "' "
                    sqlSeoulRealTrade += " , modify_date=NOW() "
                    sqlSeoulRealTrade += " WHERE unique_key='"+strUniqueKey+"' "

                    nUpdateCount = nUpdateCount + 1
                    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "sqlSeoulRealTradeUpdate > " +  str(sqlSeoulRealTrade))
                    cursorRealEstate.execute(sqlSeoulRealTrade)

                else:

                    strGOVHouseType = str(dictSeoulRealtyTradeDataMaster['HOUSE_TYPE'])
                    strTradeDBLAND_GBN_NM = str(rstSelectDatas.get('LAND_GBN_NM'))

                    if strGOVHouseType != '단독다가구' and strTradeDBLAND_GBN_NM != '블럭':

                        strTradeDBMasterBUBEON = str(dictSeoulRealtyTradeDataMaster['BUBEON']).lstrip("0")
                        if strTradeDBMasterBUBEON != '':
                            strTradeDBMasterBUBEON = "-" + strTradeDBMasterBUBEON


                        strDOROJUSO = "서울특별시 "
                        strDOROJUSO += dictSeoulRealtyTradeDataMaster['SGG_NM'] + " "
                        strDOROJUSO += dictSeoulRealtyTradeDataMaster['BJDONG_NM'] + " "
                        strDOROJUSO += str(dictSeoulRealtyTradeDataMaster['BONBEON']).lstrip("0")
                        strDOROJUSO += str(strTradeDBMasterBUBEON)

                        resultsDict = GeoDataModule.getJusoData(strDOROJUSO)
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +  "resultsDict >" +  str(type(resultsDict)) + str(resultsDict))
                        if isinstance(resultsDict, dict) == False:
                            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +  "resultsDict > False " + str(resultsDict))
                            # if type(resultsDict) is dict:
                            # raise Exception(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                            #                                    inspect.getframeinfo(inspect.currentframe()).lineno) + 'strResultCode => ' + strResultCode)  # 예외를 발생시킴
                            # break

                        else:
                            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + str(resultsDict['roadAddrPart1'].strip()))
                            strDOROJUSO = str(resultsDict['roadAddrPart1']).strip()


                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "strDOROJUSO => "+ str(strDOROJUSO))
                        resultsDict = GeoDataModule.getNaverGeoData(strDOROJUSO)
                        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "resultsDict >"  + str(resultsDict))
                        if isinstance(resultsDict, dict) == False:
                            strNaverLongitude = str(0)
                            strNaverLatitude = str(0)

                        else:
                            strNaverLongitude = str(resultsDict['x'])  # 127
                            strNaverLatitude = str(resultsDict['y'])  # 37

                    else:
                        strNaverLongitude = str(0)
                        strNaverLatitude = str(0)

                    sqlSeoulRealTrade  = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulRealTradeDataTable + " SET unique_key='"+strUniqueKey+"' ,"
                    sqlSeoulRealTrade += " ACC_YEAR='" + dictSeoulRealtyTradeDataMaster['ACC_YEAR'] + "', "
                    sqlSeoulRealTrade += " SGG_CD='" + dictSeoulRealtyTradeDataMaster['SGG_CD'] + "', "
                    sqlSeoulRealTrade += " SGG_NM='" + dictSeoulRealtyTradeDataMaster['SGG_NM'] + "', "
                    sqlSeoulRealTrade += " BJDONG_CD='" + dictSeoulRealtyTradeDataMaster['BJDONG_CD'] + "', "
                    sqlSeoulRealTrade += " BJDONG_NM='" + dictSeoulRealtyTradeDataMaster['BJDONG_NM'] + "', "
                    sqlSeoulRealTrade += " BONBEON='" + dictSeoulRealtyTradeDataMaster['BONBEON'] + "', "
                    sqlSeoulRealTrade += " BUBEON='" + dictSeoulRealtyTradeDataMaster['BUBEON'] + "', "
                    sqlSeoulRealTrade += " lng='" + strNaverLongitude + "', "
                    sqlSeoulRealTrade += " lat='" + strNaverLatitude + "', "
                    sqlSeoulRealTrade += " geo_point = ST_GeomFromText('POINT(" + strNaverLongitude + " " + strNaverLatitude + ")', 4326,'axis-order=long-lat'), "
                    sqlSeoulRealTrade += " LAND_GBN='" + dictSeoulRealtyTradeDataMaster['LAND_GBN'] + "', "
                    sqlSeoulRealTrade += " LAND_GBN_NM='" + dictSeoulRealtyTradeDataMaster['LAND_GBN_NM'] + "', "
                    sqlSeoulRealTrade += " BLDG_NM='" + dictSeoulRealtyTradeDataMaster['BLDG_NM'].replace('\'', "\\'") + "', "
                    sqlSeoulRealTrade += " HOUSE_TYPE='" + dictSeoulRealtyTradeDataMaster['HOUSE_TYPE'] + "', "
                    sqlSeoulRealTrade += " DEAL_YMD='" + dictSeoulRealtyTradeDataMaster['DEAL_YMD'] + "', "
                    sqlSeoulRealTrade += " OBJ_AMT='" + dictSeoulRealtyTradeDataMaster['OBJ_AMT'] + "', "
                    sqlSeoulRealTrade += " BLDG_AREA='" + dictSeoulRealtyTradeDataMaster['BLDG_AREA'] + "', "
                    sqlSeoulRealTrade += " TOT_AREA='" + dictSeoulRealtyTradeDataMaster['TOT_AREA'] + "', "
                    sqlSeoulRealTrade += " FLOOR='" + dictSeoulRealtyTradeDataMaster['FLOOR'] + "', "
                    sqlSeoulRealTrade += " RIGHT_GBN='" + dictSeoulRealtyTradeDataMaster['RIGHT_GBN'] + "', "
                    sqlSeoulRealTrade += " CNTL_YMD='" + dictSeoulRealtyTradeDataMaster['CNTL_YMD'] + "', "
                    sqlSeoulRealTrade += " BUILD_YEAR='" + dictSeoulRealtyTradeDataMaster['BUILD_YEAR'] + "', "
                    sqlSeoulRealTrade += " state='" + strState + "', "
                    sqlSeoulRealTrade += " REQ_GBN='" + dictSeoulRealtyTradeDataMaster['REQ_GBN'] + "', "
                    sqlSeoulRealTrade += " RDEALER_LAWDNM='" + dictSeoulRealtyTradeDataMaster['ACC_YEAR'] + "' "
                    cursorRealEstate.execute(sqlSeoulRealTrade)

                    nInsertedCount = nInsertedCount + 1
                logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "COMMIT > " + str(sqlSeoulRealTrade))
                ResRealEstateConnection.commit()


            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = dtProcessDay
            dictSwitchData['data_2'] = nLoop
            dictSwitchData['data_3'] = nStartNumber
            dictSwitchData['data_4'] = nEndNumber
            dictSwitchData['data_5'] = nUpdateCount
            dictSwitchData['data_6'] = nInsertedCount
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            #for list in jsonRowDatas:
            logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "dictSeoulRealtyTradeDataMaster" + "====================================================")
            nStartNumber = nEndNumber + 1
            nEndNumber = nEndNumber + nProcessedCount
            ResRealEstateConnection.close()

        #while True:
        logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +  "End While")
        time.sleep(1)
    # For nProcessYear:

    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +  "End nProcessYears")

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '00'
    dictSwitchData['data_1'] = dtProcessDay
    dictSwitchData['data_2'] = strProcessDay
    dictSwitchData['data_3'] = nStartNumber
    dictSwitchData['data_4'] = nEndNumber
    dictSwitchData['data_5'] = dtProcessDay
    dictSwitchData['data_6'] = nInsertedCount
    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

except Exception as e:
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +  "Error Exception")
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +  str(e))
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + str(type(e)))

    # Switch 오류 업데이트
    dictSeoulSwitch = {}
    dictSeoulSwitch['seq'] = dtProcessDay
    dictSeoulSwitch['state'] = 20
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "dtProcessDay >"+ str(dictSeoulSwitch))
    bSwitchUpdateResult = LibSeoulRealTradeSwitch.SwitchSeoulUpdate(dictSeoulSwitch)
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + "bSwitchUpdateResult >" + str(bSwitchUpdateResult))

    # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
    dictSwitchData = dict()
    dictSwitchData['result'] = '30'
    if dtProcessDay is not None:
        dictSwitchData['data_1'] = dtProcessDay

    if strProcessDay is not None:
        dictSwitchData['data_2'] = strProcessDay

    if nStartNumber is not None:
        dictSwitchData['data_3'] = nStartNumber

    if nEndNumber is not None:
        dictSwitchData['data_3'] = nEndNumber

    LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

else:
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + ' Inserted => '+ str(nInsertedCount)+ ' , Updated => '+ str(nUpdateCount))
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) + " ========================= SUCCESS END")
finally:
    logging.info(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                   inspect.getframeinfo(inspect.currentframe()).lineno) +  " Finally END")



