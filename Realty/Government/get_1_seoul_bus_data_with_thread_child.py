import sys
import os
import time
import datetime
import math
import traceback
import urllib.request
import json


from datetime import datetime as DateTime, timedelta as TimeDelta
from Init.Functions.Logs import GetLogDef
from Lib.CustomException import QuitException
from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from Helper import hash_fnc
from Lib.RDB import pyMysqlConnector


sys.path.append("D:/PythonProjects/airstock")
import urllib.request
import json
import pymysql
import traceback
from datetime import datetime as DateTime, timedelta as TimeDelta

from Init.Functions.Logs import GetLogDef


def test_thread(nBaseDate, nCallStartCount, nCallEndCount, dictSeoulColumnInfoData):

    try:
        # # nBaseDate = 20221231
        # dtToday = DateTime.today()
        #
        # print("[", os.getpid(), "]", "dtToday >", type(dtToday), dtToday)
        # print("[" + str(os.getpid()) + "]" + "nStartCount >" + str(type(nStartCount)) + str(nStartCount))
        # time.sleep(2)
        # print("\\\n[" + str(os.getpid()) + "]" + "nEndCount >" + str(type(nEndCount)) + str(nEndCount))
        nProcessID = os.getpid()
        # print(GetLogDef.lineno(), "[", nProcessID, "]", "nBaseDate > ", type(nBaseDate), nBaseDate)
        # print(GetLogDef.lineno(), "[", nProcessID, "]", "nCallStartCount > ", type(nCallStartCount), nCallStartCount)
        # print(GetLogDef.lineno(), "[", nProcessID, "]", "nCallEndCount > ", type(nCallEndCount), nCallEndCount)

        url = "http://openapi.seoul.go.kr:8088/" + init_conf.SeoulAuthorizationKey + "/json/CardBusStatisticsServiceNew/" + str(nCallStartCount) + "/" + str(nCallEndCount) + "/" + str(nBaseDate)
        print(GetLogDef.lineno(__file__), "[", nProcessID, "]", "url > ", url)


        # url을 불러오고 이것을 인코딩을 utf-8로 전환하여 결과를 받자.
        response = urllib.request.urlopen(url)
        json_str = response.read().decode("utf-8")

        # 받은 데이터가 문자열이라서 이를 json으로 변환한다.
        json_object = json.loads(json_str)

        print(GetLogDef.lineno(__file__), "json_object.get('RESULT')  > ", json_object.get('RESULT'))

        # 조회된 데이터가 "있으면" json_object.get('RESULT') => None
        if json_object.get('RESULT') != None:
            print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('CODE'))
            print(GetLogDef.lineno(__file__), json_object.get('RESULT').get('MESSAGE'))
            Exception(GetLogDef.lineno(__file__))  # 예외를 발생시킴

        bMore = json_object.get('CardBusStatisticsServiceNew')
        if bMore is None:
            Exception(GetLogDef.lineno(__file__), 'bMore => ', bMore)  # 예외를 발생시킴

        nTotalCount = bMore.get('list_total_count')
        print("nTotalCount > ", nTotalCount)

        jsonResultDatas = bMore.get('RESULT')
        jsonResultDatasResult = jsonResultDatas

        strResultCode = jsonResultDatasResult.get('CODE')
        strResultMessage = jsonResultDatasResult.get('MESSAGE')

        if strResultCode != 'INFO-000':
            Exception(GetLogDef.lineno(), 'strResultCode => ', strResultCode, strResultMessage)  # 예외를 발생시킴
            # GetOut while True:

        jsonRowDatas = bMore.get('row')

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        nLoop=0
        for list in jsonRowDatas:
            print("[ "+str(nCallStartCount)+" - "+str(nCallEndCount)+" ][ "+str(nLoop)+" ] ")
            nLoop += 1

            dictSeoulRealtyTradeDataMaster = {}

            for dictSeoulMasterKeys in list.keys():

                for dictNaverMobileMasterKeys in list.keys():
                    dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = list.get(dictSeoulMasterKeys)

                # Non-strings are converted to strings.
                if type(dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys]) is not str:
                    dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys] = str(
                        dictSeoulRealtyTradeDataMaster[dictSeoulMasterKeys])

            # for dictSeoulMasterKeys in list.keys():

            strUniqueKey = dictSeoulRealtyTradeDataMaster['USE_DT'] + "_"
            strUniqueKey += dictSeoulRealtyTradeDataMaster['BUS_ROUTE_ID'] + "_"
            strUniqueKey += dictSeoulRealtyTradeDataMaster['STND_BSST_ID'] + "_"
            strUniqueKey += hash_fnc.MD5EncodeFunction(dictSeoulRealtyTradeDataMaster['BUS_STA_NM'])

            strInsertYear = str(dictSeoulRealtyTradeDataMaster['USE_DT'])[0:4]

            # INSERT
            qryInfoInsert = " INSERT INTO kt_realty_seoul_bus_using_static_"+strInsertYear+" SET "
            qryInfoInsert += " unique_key = '" + str(strUniqueKey) + "', "

            for dictSeoulAptInfoDataMaster in dictSeoulRealtyTradeDataMaster.keys():

                # print("dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster]", dictSeoulAptInfoDataMaster, dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster], type(dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster]))
                # fields 타입이 int 면 empty 인 경우 예외 처리 - 타입오류 예방
                if dictSeoulColumnInfoData[dictSeoulAptInfoDataMaster]['type'] == 'int' and dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] == '':
                    dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] = '0'

                if dictSeoulColumnInfoData[dictSeoulAptInfoDataMaster]['type'] == 'datetime' and dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] == '':
                    dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] = '0000-00-00 00:00:00'

                qryInfoInsert += " " + dictSeoulAptInfoDataMaster + "='" + dictSeoulRealtyTradeDataMaster[dictSeoulAptInfoDataMaster] + "', "

            qryInfoInsert += "modify_date=NOW(),"
            qryInfoInsert += "reg_date=NOW()"
            print("qryInfoInsert", qryInfoInsert, type(qryInfoInsert))
            cursorRealEstate.execute(qryInfoInsert)
            ResRealEstateConnection.commit()













    except Exception as e:

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        print(GetLogDef.lineno(__file__), "Error Exception")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))

    except QuitException as e:

        # # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        # dictSwitchData = dict()
        # dictSwitchData['result'] = '30'
        # LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)

        print(GetLogDef.lineno(__file__), "QuitException")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))


    else:
        print("========================= SUCCESS END")

    finally:
        print("Finally END")
        # ResRealEstateConnection.close()





