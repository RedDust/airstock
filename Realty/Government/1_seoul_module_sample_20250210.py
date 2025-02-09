# 지역 실거래 데이터 - 서울(41) 버스 정류장 위도경도 데이터 (75)
# 2025-02-10 커밋
# https://data.seoul.go.kr/dataList/OA-12912/S/1/datasetView.do


import sys
import json
import time
import random
import pymysql
import logging
import logging.handlers
import inspect
import traceback
import re

sys.path.append("D:/PythonProjects/airstock")

import urllib.request
import json
import pymysql
import datetime, time, inspect

from Lib.RDB import pyMysqlConnector

from Init.Functions.Logs import GetLogDef

from datetime import datetime as DateTime, timedelta as TimeDelta
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
from Lib.CustomException.QuitException import QuitException


def main():
    try:

        print("====================== TRY START")

        strProcessType = '034175'
        data_1 = '00'
        data_2 = '00'
        data_3 = '00'
        data_4 = '00'
        data_5 = '00'
        data_6 = '00'

        dtNow = DateTime.today()
        LogPath = 'Government/' + strProcessType
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)
        intWeekDay = dtNow.weekday()

        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()),
                     "[CRONTAB START]============================================================")

        # 스위치 데이터 조회 type(20=법원경매물건 수집) result (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        rstResult = LibNaverMobileMasterSwitchTable.SwitchResultSelectV2(strProcessType)
        strResult = rstResult.get('result')
        if strResult is False:
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'strResult => ' + str(strResult))  # 예외를 발생시킴

        if strResult == '10':
            quit(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + 'It is currently in operation. => ' + str(
                strResult))  # 예외를 발생시킴

        if strResult == '20':
            strAddressSiguSequence = str(rstResult.get('data_1'))

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '10'
        dictSwitchData['data_1'] = data_1
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, True, dictSwitchData)

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '00'
        dictSwitchData['today_work'] = '1'
        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)




    except Exception as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        data_6 = err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)




    except QuitException as e:

        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)
        dictSwitchData = dict()
        dictSwitchData['result'] = '20'

        data_6 = err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

        if data_1 is not None:
            dictSwitchData['data_1'] = data_1
        if data_2 is not None:
            dictSwitchData['data_2'] = data_2
        if data_3 is not None:
            dictSwitchData['data_3'] = data_3
        if data_4 is not None:
            dictSwitchData['data_4'] = data_4
        if data_5 is not None:
            dictSwitchData['data_5'] = data_5
        if data_6 is not None:
            dictSwitchData['data_6'] = data_6

        LibNaverMobileMasterSwitchTable.SwitchResultUpdateV2(strProcessType, False, dictSwitchData)




    else:
        print("========================= SUCCESS END")

    finally:
        print("Finally END")
        # ResRealEstateConnection.close()


if __name__ == '__main__':
    main()
