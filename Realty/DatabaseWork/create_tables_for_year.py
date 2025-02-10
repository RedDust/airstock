import sys
import json
import time
import random
import pymysql
import inspect
import requests
import traceback
import re

sys.path.append("D:/PythonProjects/airstock")

import urllib.request
import json
import pymysql
import datetime, time, inspect


from Lib.RDB import pyMysqlConnector


from datetime import datetime as DateTime, timedelta as TimeDelta
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Realty.Government.Lib.Logging import UnifiedLogDeclarationFunction as ULF
from Lib.CustomException.QuitException import QuitException

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from dateutil.relativedelta import relativedelta



def main():

    try:
        print("====================== TRY START")
        dtNow = DateTime.today()
        LogPath = 'DatabaseWork/create_table_for_year'
        setLogger = ULF.setLogFile(dtNow, logging, LogPath)



        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe())+ "[CRONTAB START]=============================")

        #연테이블
        dictCreateYearTables = dict()
        dictCreateYearTables[0] = dict()
        dictCreateYearTables[0]['origin'] = 'kt_realty_seoul_bus_using_static_master'
        dictCreateYearTables[0]['target'] = 'kt_realty_seoul_bus_using_static_YYYY'

        dictCreateYearTables[1] = dict()
        dictCreateYearTables[1]['origin'] = 'kt_realty_molit_statistics_apt_master_year'
        dictCreateYearTables[1]['target'] = 'kt_realty_molit_statistics_apt_master_year_YYYY'

        dictCreateYearTables[2] = dict()
        dictCreateYearTables[2]['origin'] = 'kt_realty_molit_real_trade_backup'
        dictCreateYearTables[2]['target'] = 'kt_realty_molit_real_trade_backup_YYYY'

        dictCreateYearTables[3] = dict()
        dictCreateYearTables[3]['origin'] = 'kt_realty_molit_real_rent_backup'
        dictCreateYearTables[3]['target'] = 'kt_realty_molit_real_rent_backup_YYYY'

        dictCreateYearTables[4] = dict()
        dictCreateYearTables[4]['origin'] = 'kt_realty_molit_house_real_rent_backup_2024'
        dictCreateYearTables[4]['target'] = 'kt_realty_molit_house_real_rent_backup_YYYY'


        # 월 테이블

        dictCreateMonthTables = dict()
        dictCreateMonthTables[0] = dict()
        dictCreateMonthTables[0]['origin'] = 'kt_realty_naver_mobile_backup_202412'
        dictCreateMonthTables[0]['target'] = 'kt_realty_naver_mobile_backup_YYYYMM'


        #DB 연결 선언
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)


        #연 테이블 생성

        intNowYear = int(dtNow.year)
        strNowYear = str(intNowYear)
        intNextYear = intNowYear + 1
        strNextYear = str(intNextYear)

        for keyCreateTableKey, dictCreateTableValue in dictCreateYearTables.items():
            print("keyCreateTableKey => ", keyCreateTableKey, " =>", dictCreateTableValue)

            print("origin => ", dictCreateTableValue['origin'])
            print("target => ", dictCreateTableValue['target'])

            strOriginTable = str(dictCreateTableValue['origin'])
            # NOW
            strTargetNowYearTable = str(dictCreateTableValue['target']).replace("YYYY", strNowYear)
            # NEXT
            strTargetNowNextTable = str(dictCreateTableValue['target']).replace("YYYY", strNextYear)

            print("strOriginTable => ", strOriginTable)
            print("strTargetNowYearTable => ", strTargetNowYearTable)
            print("strTargetNowNextTable => ", strTargetNowNextTable)

            print("strTargetNowYearTable===========>", strTargetNowYearTable)


            sqlSelectTable = " SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            sqlSelectTable += " WHERE TABLE_SCHEMA = 'kt_real_estate_auction' "
            sqlSelectTable += " AND TABLE_NAME =  %s "
            cursorRealEstate.execute(sqlSelectTable,(strTargetNowYearTable))
            strSelectedCount = int(cursorRealEstate.rowcount)
            print("NowYearTable strSelectedCount => ", strSelectedCount, strTargetNowYearTable)

            if strSelectedCount < 1:
                sqlCreateTable = " CREATE TABLE " + strTargetNowYearTable
                sqlCreateTable += " LIKE " + strOriginTable
                print("sqlCreateTable => ", sqlCreateTable)
                cursorRealEstate.execute(sqlCreateTable)


            print("strTargetNowNextTable===========>", strTargetNowNextTable)

            sqlSelectTable = " SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            sqlSelectTable += " WHERE TABLE_SCHEMA = 'kt_real_estate_auction' "
            sqlSelectTable += " AND TABLE_NAME =  %s "
            cursorRealEstate.execute(sqlSelectTable, (strTargetNowNextTable))
            strSelectedCount = int(cursorRealEstate.rowcount)
            print("NowNextTable strSelectedCount => ", strSelectedCount, strTargetNowNextTable)

            if strSelectedCount < 1:
                sqlCreateTable = " CREATE TABLE " + strTargetNowNextTable
                sqlCreateTable += " LIKE " + strOriginTable
                print("sqlCreateTable => ", sqlCreateTable)
                cursorRealEstate.execute(sqlCreateTable)

            ResRealEstateConnection.commit()

        #월 테이블
        for keyCreateTableKey, dictCreateTableValue in dictCreateMonthTables.items():
            print("keyCreateTableKey => ", keyCreateTableKey, " =>", dictCreateTableValue)

            print("origin => ", dictCreateTableValue['origin'])
            print("target => ", dictCreateTableValue['target'])


            for i in range(1, 15):
                dtTargetMonth = dtNow + relativedelta(months=i)
                # print("dtTargetMonth===========>", dtTargetMonth)

                strNowYear = str(dtTargetMonth.year).zfill(4) + str(dtTargetMonth.month).zfill(2)

                strOriginTable = str(dictCreateTableValue['origin'])
                # NOW
                strTargetNowYearTable = str(dictCreateTableValue['target']).replace("YYYYMM", strNowYear)

                print("strOriginTable => ", strOriginTable)
                print("strTargetNowYearTable => ", strTargetNowYearTable)

                sqlSelectTable = " SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
                sqlSelectTable += " WHERE TABLE_SCHEMA = 'kt_real_estate_auction' "
                sqlSelectTable += " AND TABLE_NAME =  %s "
                cursorRealEstate.execute(sqlSelectTable, (strTargetNowYearTable))
                strSelectedCount = int(cursorRealEstate.rowcount)
                print("NowYearTable strSelectedCount => ", strSelectedCount, strTargetNowYearTable)

                if strSelectedCount < 1:
                    sqlCreateTable = " CREATE TABLE " + strTargetNowYearTable
                    sqlCreateTable += " LIKE " + strOriginTable
                    print("sqlCreateTable => ", sqlCreateTable)
                    cursorRealEstate.execute(sqlCreateTable)

            ResRealEstateConnection.commit()



    except Exception as e:
        print("========================= Exception END")
        err_msg = traceback.format_exc()
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[=============[Error Exception]")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[e : (" + str(e) + ")")
        logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[err_msg : (" + str(err_msg) + ")")

    else:
        print("========================= SUCCESS END")

    finally:
        print("Finally END")


if __name__ == '__main__':
    main()
