# 지역 실거래 데이터 - 서울(41) 버스 정류장 위도경도 데이터 (75)
# 2025-02-10 커밋
#https://data.seoul.go.kr/dataList/OA-12912/S/1/datasetView.do


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
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.GeoDataModule import GeoDataModule
import inspect as Isp, logging, logging.handlers
from Init.Functions.Logs import GetLogDef as SLog
from Realty.Government.Lib.Logging import UnifiedLogDeclarationFunction as ULF
from Lib.CustomException.QuitException import QuitException

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV
from dateutil.relativedelta import relativedelta

def GetLastData(cursorRealEstate, dtNow):

    strInsertYear = str(dtNow.year)

    while True:

        print("strInsertYear => ", strInsertYear)

        sqlSelectLastDay = "SELECT * FROM kt_realty_seoul_bus_using_static_" + strInsertYear + " ORDER BY USE_YMD DESC LIMIT 1"
        cursorRealEstate.execute(sqlSelectLastDay)
        intSelectedCount = cursorRealEstate.rowcount
        rstSeoulBusGeoData = cursorRealEstate.fetchone()
        print("sqlSelectLastDay ==>  => ", sqlSelectLastDay)
        print("intSelectedCount => ", intSelectedCount)

        if intSelectedCount > 0:
            break

        dtNow = dtNow - relativedelta(years=1)
        strInsertYear = str(dtNow.year)

    print("rstSeoulBusGeoData => ", rstSeoulBusGeoData)
    strDBUSE_YMD = rstSeoulBusGeoData.get('USE_YMD')
    return strDBUSE_YMD







def main():

    print("rstSeoulBusGeoData => main")


def UpdateCount():
    print("rstSeoulBusGeoData => UpdateCount")


if __name__ == '__main__':
    main()
    UpdateCount()


