#stock_koreainvest_test


# 금융위원회_주식시세정보
# 2024-11-25 커밋
#https://www.data.go.kr/iim/api/selectAPIAcountView.do
#https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo

import sys ,os
import time

sys.path.append("D:/PythonProjects/airstock")


import urllib.request, requests
import json
import pymysql
import traceback
import xml
import xml.etree.ElementTree as ET
import pandas as pd

from Realty.Government.Init import init_conf
from Stock.LIB.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from datetime import datetime as DateTime, timedelta as TimeDelta
import datetime
from Stock.CONFIG import ConstTableName

from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Lib.CustomException.QuitException import QuitException

def main():

    from Stock.LIB.Functions.Switch import StockSwitchTable
    import inspect as Isp, logging, logging.handlers
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
    from Stock.LIB.Functions.Table import ManufactureItemTable as MIT
    import Stock.API.koreaInvestment.Lib.kis_auth as ka
    import Stock.API.koreaInvestment.Lib.kis_domstk as kb

    dtNow = DateTime.today()

    strBaseYYYY = str(dtNow.year).zfill(4)
    strBaseMM = str(dtNow.month).zfill(2)
    strBaseDD = str(dtNow.day).zfill(2)
    strBaseHH = str(dtNow.hour).zfill(2)
    strBaseII = str(dtNow.minute).zfill(2)
    strBaseSS = str(dtNow.second).zfill(2)


    strNowDate = strBaseYYYY + strBaseMM + strBaseDD
    strNowTime = strBaseHH + ":" + strBaseII + ":" + strBaseSS
    strNowYYYYMMDD = strNowDate
    strNowHHMM = strBaseHH + strBaseII

    # strNowYYYYMMDD = strNowDate = "20241206"

    # 초기값
    strProcessType = '000000'
    strDBSequence = '0'
    strDBSectorsName = ''
    intItemLoop = 0
    strItemDatabaseName='koreainvest'

    strAddLogPath = os.path.basename(Isp.getframeinfo(Isp.currentframe()).filename).split('.')[0]
    LogPath = 'Stock/'+strAddLogPath+'/'+ strProcessType

    setLogger = ULF.setLogFile(dtNow, logging, LogPath)
    logging.info(SLog.Ins(Isp.getframeinfo, Isp.currentframe()) + "[CRONTAB START : =====================================]")

    # # DB 연결
    ResStockFriendsConnection = pyMysqlConnector.StockFriendsConnection()
    cursorStockFriends = ResStockFriendsConnection.cursor(pymysql.cursors.DictCursor)


    KoreaInvestItemsConnection = pyMysqlConnector.KoreaInvestItemsConnection()
    cursorKoreaInvestItems = KoreaInvestItemsConnection.cursor(pymysql.cursors.DictCursor)

    sqlSelectRecommendation = "SELECT * FROM stockfriends_koreainvest_recommendation_4 "
    cursorStockFriends.execute(sqlSelectRecommendation)
    intRecommendCount = cursorStockFriends.rowcount

    if intRecommendCount > 1:

        rstSelectDatas = cursorStockFriends.fetchall()
        if rstSelectDatas == None:
            raise QuitException(SLog.Ins(Isp.getframeinfo, Isp.currentframe()))  # 예외를 발생시킴


        for rstSelectData in rstSelectDatas:

            strNowYYYYMMDD = str(rstSelectData.get('YYYYMMDD'))
            strEndHHMM = str(rstSelectData.get('HHII'))
            strRecommendPoint = str(rstSelectData.get('recommend_point'))
            strDBItemCode = str(rstSelectData.get('sectors_code'))
            strDBSectorsName = str(rstSelectData.get('sectors_name'))
            a_stck_prpr = str(rstSelectData.get('price'))
            a_stck_hgpr = str(rstSelectData.get('max_price'))
            b_frgn_ntby_qty = str(rstSelectData.get('b_frgn_ntby_qty'))
            a_frgn_ntby_qty = str(rstSelectData.get('a_frgn_ntby_qty'))
            b_pgtr_ntby_qty = str(rstSelectData.get('b_pgtr_ntby_qty'))
            a_pgtr_ntby_qty = str(rstSelectData.get('a_pgtr_ntby_qty'))
            b_acml_vol = str(rstSelectData.get('b_acml_vol'))
            a_acml_vol = str(rstSelectData.get('a_acml_vol'))

            sqlInsertRecommendation = " INSERT INTO stockfriends_koreainvest_recommendation_4 SET "
            sqlInsertRecommendation += " YYYYMMDD =%s "
            sqlInsertRecommendation += " , HHII =%s "
            sqlInsertRecommendation += " , recommend_point =%s "
            sqlInsertRecommendation += " , sectors_code =%s "
            sqlInsertRecommendation += " , sectors_name =%s "

            sqlInsertRecommendation += " , price =%s "
            sqlInsertRecommendation += " , max_price =%s "
            sqlInsertRecommendation += " , b_frgn_ntby_qty =%s "
            sqlInsertRecommendation += " , a_frgn_ntby_qty =%s "
            sqlInsertRecommendation += " , b_pgtr_ntby_qty =%s "

            sqlInsertRecommendation += " , a_pgtr_ntby_qty =%s "
            sqlInsertRecommendation += " , b_acml_vol =%s "
            sqlInsertRecommendation += " , a_acml_vol =%s "

            print(strNowYYYYMMDD, strEndHHMM, strRecommendPoint, strDBItemCode, strDBSectorsName, a_stck_prpr, a_stck_hgpr, b_frgn_ntby_qty, a_frgn_ntby_qty, b_pgtr_ntby_qty, a_pgtr_ntby_qty, b_acml_vol, a_acml_vol)

            cursorStockFriends.execute(sqlInsertRecommendation, (strNowYYYYMMDD,strEndHHMM, strRecommendPoint, strDBItemCode,strDBSectorsName,a_stck_prpr,a_stck_hgpr,b_frgn_ntby_qty,a_frgn_ntby_qty,b_pgtr_ntby_qty,a_pgtr_ntby_qty,b_acml_vol,a_acml_vol))
            ResStockFriendsConnection.commit()


        print("[316]=====================================================================================")




if __name__ == '__main__':
    main()