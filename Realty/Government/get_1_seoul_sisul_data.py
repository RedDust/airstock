# 서울시 건설공사 추진 현황
# 서울시(투출, 자치구 포함)에서 발주 한 시설공사 공정관리 및 일반공사 정보로 공정율, 사업비, 공사기간, 사업규모, 계약현황 등이 제공됩니다.
# 2023-01-30 커밋
#https://data.seoul.go.kr/dataList/OA-2540/S/1/datasetView.do


import sys
sys.path.append("D:/PythonProjects/airstock")


import urllib.request
import json
import pymysql
import datetime

import pandas as pd
from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from Lib.RDB import pyMysqlConnector
import traceback

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable

import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF


def main():
    try:

        # 한번에 처리할 건수
        nProcessedCount = 1000

        nTotalCount = 0

        # 실제[]
        nLoopTotalCount = 0

        # 시작번호
        nStartNumber = 1

        # 최종번호
        nEndNumber = nProcessedCount

        #서울 부동산 실거래가 데이터 - 임대차
        strProcessType = '034190'
        KuIndex = '00'
        arrCityPlace = '00'
        targetRow = '00'
        nAddressSiguSequence = '0'
        dtNow = DateTime.today()
        # print(dtNow.hour)
        # print(dtNow.minute)
        # print(dtNow)

        LogPath = 'Government/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()

        print("[LogPath]" + LogPath )

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()),"[START=======================================================]")

        print("[SLog.Ins(Isp.getframeinfo, Isp.currentframe()]" + SLog.Ins(Isp.getframeinfo, Isp.currentframe()))



        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = KuIndex
        dictSwitchData['data_2'] = arrCityPlace
        dictSwitchData['data_3'] = targetRow
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) , dictSwitchData)

        while True:

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ " [nStartNumber: (" + str(nStartNumber)+ ")")
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ " [nEndNumber: (" + str(nEndNumber)+ ")")

            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), dictSwitchData)

            # url 변수에 최종 완성본 url을 넣자   1  부터 10번까지 ORDER 는 뒷 쪽부터 ( 최근부터) 5/10 하면 5,6,7,8,9,10 6개 나옮
            url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/ListOnePMISBizInfo/" + str(
                nStartNumber) + "/" + str(nEndNumber)

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                         "[url: (" + str(url) + ")")

            print(SLog.Ins(Isp.getframeinfo, Isp.currentframe()), url)

            # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
            response = urllib.request.urlopen(url)
            json_str = response.read().decode("utf-8")

            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                         "[json_str: (" + str(json_str) + ")")

            # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
            json_object = json.loads(json_str)
            logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                         "[json_object: (" + str(json_object) + ")")

            bMore = json_object.get('ListOnePMISBizInfo')

            if bMore is None:
                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +"[json_object: (" + str(json_object) + ")")
                break

            # DB 연결
            ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
            cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

            nTotalCount = bMore.get('list_total_count')
            jsonResultDatas = bMore.get('RESULT')
            jsonResultDatasResult = jsonResultDatas

            strResultCode = jsonResultDatasResult.get('CODE')
            strResultMessage = jsonResultDatasResult.get('MESSAGE')

            if strResultCode != 'INFO-000':
                raise Exception(
                    SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResultCode => [' + str(strResultCode) + ']')  # 예외를 발생시킴
                # GetOut while True:

            jsonRowDatas = bMore.get('row')

            # 3. 건별 처리
            logging.info(SLog.Ins(Isp.getframeinfo,
                                  Isp.currentframe()) + "[Processing]######################################################")
            nInsertedCount = 0
            nUpdateCount = 0
            nLoop = 0
            strUniqueKey = ''

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '10'
            dictSwitchData['data_1'] = url
            dictSwitchData['data_2'] = strResultCode
            dictSwitchData['data_3'] = nStartNumber
            dictSwitchData['data_4'] = nEndNumber
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

            for list in jsonRowDatas:
                # print("[ " + str(nStartNumber) + " - " + str(nEndNumber) + " ][ " + str(nLoop) + " ] ")
                nLoop += 1

                dictSeoulRealtyTradeDataMaster = {}

                for dictSeoulMasterKeys in list.keys():

                    for dictNaverMobileMasterKeys in list.keys():
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = list.get(dictSeoulMasterKeys)

                    # Non-strings are converted to strings.
                    if type(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys]) is not str:
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = str(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys])


                cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)
                qrySelectSeoulTradeMaster = "SELECT * FROM " + ConstRealEstateTable_GOV.SeoulSisulDataTable + "  WHERE  PJT_CD=%s"

                cursorRealEstate.execute(qrySelectSeoulTradeMaster, dictSeoulRealtyTradeDataMaster['BIZ_CD'])
                row_result = cursorRealEstate.rowcount
                #이미 저장 되어 있으면 더이상 저장 하지 않는다.
                if row_result > 0:
                    continue

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                             "[dictSeoulRealtyTradeDataMaster: (" + str(dictSeoulRealtyTradeDataMaster) + ")")



                arrCNTRCT_PRD = []
                # 계약기간이 명시 되어 있지 않으면 예외처리
                if len(dictSeoulRealtyTradeDataMaster['CSTRN_PRD']) < 2:
                    arrCNTRCT_PRD = [str(0000-00-00), str(0000-00-00)]
                else:
                    arrCNTRCT_PRD = dictSeoulRealtyTradeDataMaster['CSTRN_PRD'].split("~")


                if dictSeoulRealtyTradeDataMaster.get('WEB_CMR_RTSP_ADDR') is not None:
                    WEB_CMR_RTSP_ADDR = dictSeoulRealtyTradeDataMaster['WEB_CMR_RTSP_ADDR'].replace('\'', "\\'")
                else:
                    WEB_CMR_RTSP_ADDR = ''

                sqlSeoulRealTrade = " INSERT INTO " + ConstRealEstateTable_GOV.SeoulSisulDataTable + " SET " \
                                    " PJT_CD='" + dictSeoulRealtyTradeDataMaster['BIZ_CD'] + "', " \
                                    " PJT_NAME='" + dictSeoulRealtyTradeDataMaster['BIZ_NM'].replace('\'', "\\'") + "', " \
                                    " PJT_FIN_YN='" + dictSeoulRealtyTradeDataMaster['CMCN_YN1'] + "', " \
                                    " PJT_FIN_YN_NM='" + dictSeoulRealtyTradeDataMaster['CMCN_YN2'] + "', " \
                                    " PLAN_RT='" + dictSeoulRealtyTradeDataMaster['PROCS_PLAN'] + "', " \
                                    " REAL_RT='" + dictSeoulRealtyTradeDataMaster['PROCS_PRFMNC'] + "', " \
                                    " PER_RT='" + dictSeoulRealtyTradeDataMaster['PER_RT'] + "', " \
                                    " BASIC_DT='" + dictSeoulRealtyTradeDataMaster['CRTR_YMD'] + "', " \
                                    " DT1='" + dictSeoulRealtyTradeDataMaster['DAY_TOT'] + "', " \
                                    " DT2='" + dictSeoulRealtyTradeDataMaster['DAY_ELPS'] + "', " \
                                    " DT3='" + dictSeoulRealtyTradeDataMaster['DAY_JOB'] + "', " \
                                    " TOT_CNTRT_AMT='" + dictSeoulRealtyTradeDataMaster['AMT_CTRT'] + "', " \
                                    " TOT_PJT_AMT='" + dictSeoulRealtyTradeDataMaster['AMT_BIZ'] + "', " \
                                    " DU_DATE_START='" + arrCNTRCT_PRD[0] + "', " \
                                    " DU_DATE_END='" + arrCNTRCT_PRD[1] + "', " \
                                    " OFFICE_ADDR='" + dictSeoulRealtyTradeDataMaster['CSTRN_PSTN'].replace('\'', "\\'") + "', " \
                                    " LAT='" + dictSeoulRealtyTradeDataMaster['LAT'] + "', " \
                                    " LNG='" + dictSeoulRealtyTradeDataMaster['LOT'] + "', " \
                                    " USER_1='" + dictSeoulRealtyTradeDataMaster['PIC_PE_NM'].replace('\'', "\\'") + "', " \
                                    " USER_2='" + dictSeoulRealtyTradeDataMaster['SPVS_PE_NM'].replace('\'', "\\'") + "', " \
                                    " USER_3='" + dictSeoulRealtyTradeDataMaster['AGT_PE_NM'].replace('\'', "\\'") + "', " \
                                    " ORG_1='" + dictSeoulRealtyTradeDataMaster['INST_NM'].replace('\'', "\\'") + "', " \
                                    " ORG_2='" + dictSeoulRealtyTradeDataMaster['SPVS_NM'].replace('\'', "\\'") + "', " \
                                    " ORG_3='" + dictSeoulRealtyTradeDataMaster['CNST_ENT'].replace('\'', "\\'") + "', " \
                                    " PJT_SCALE='" + dictSeoulRealtyTradeDataMaster['BIZ_SCL'].replace('\'', "\\'") + "', " \
                                    " RTSP_ADDR='" + WEB_CMR_RTSP_ADDR + "', " \
                                    " CNRT_ADDR='" + dictSeoulRealtyTradeDataMaster['CTRT_INFO_URL'].replace('\'', "\\'") + "', " \
                                    " BILLPAY_ADDR='" + dictSeoulRealtyTradeDataMaster['IMPL_INFO_URL'].replace('\'', "\\'") + "', " \
                                    " AIR_VIEW_IMG='" + dictSeoulRealtyTradeDataMaster['ARLV_IMG_URL'].replace('\'', "\\'") + "', " \
                                    " ALIMI_ADDR='" + dictSeoulRealtyTradeDataMaster['CSTRN_OTLN'].replace('\'', "\\'") + "' "

                logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) +
                             "[sqlSeoulRealTrade: (" + str(sqlSeoulRealTrade) + ")")

                cursorRealEstate.execute(sqlSeoulRealTrade)
                nInsertedCount = nInsertedCount + 1

                ResRealEstateConnection.commit()

            nStartNumber = nEndNumber + 1
            nEndNumber = nEndNumber + nProcessedCount
            ResRealEstateConnection.close()

            # 스위치 데이터 업데이트 (10:처리중, 00:시작전(처리완료), 20:오류 , 30:시작준비 - start_time 기록)
            dictSwitchData = dict()
            dictSwitchData['result'] = '00'
            dictSwitchData['data_1'] = dictSeoulRealtyTradeDataMaster['BIZ_CD']
            dictSwitchData['data_2'] = dictSeoulRealtyTradeDataMaster['BIZ_NM'].replace('\'', "\\'")
            dictSwitchData['data_3'] = nStartNumber
            dictSwitchData['data_4'] = nEndNumber
            dictSwitchData['data_5'] = dictSeoulRealtyTradeDataMaster['INST_NM'].replace('\'', "\\'")
            dictSwitchData['data_6'] = nInsertedCount
            LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)


    except Exception as e:

        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '30'
        if dictSeoulRealtyTradeDataMaster['BIZ_CD'] is not None:
            dictSwitchData['data_1'] = dictSeoulRealtyTradeDataMaster['BIZ_CD']

        if dictSeoulRealtyTradeDataMaster['BIZ_NM'].replace('\'', "\\'") is not None:
            dictSwitchData['data_2'] = dictSeoulRealtyTradeDataMaster['BIZ_NM'].replace('\'', "\\'")

        if nStartNumber is not None:
            dictSwitchData['data_3'] = nStartNumber

        if nEndNumber is not None:
            dictSwitchData['data_3'] = nEndNumber

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

    else:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[SUCCESS END]")

    finally:
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Finally END]")



if __name__ == '__main__':
    main()