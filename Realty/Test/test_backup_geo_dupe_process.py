#test_backup_geo_dupe_process
# 경매데이터 중복된 백업 데이터



import requests

# This is a sample Python script.
import sys
import json
import time
import random
import pymysql
import datetime
sys.path.append("D:/PythonProjects/airstock")
import urllib.request
import logging
import logging.handlers
import inspect
import traceback

from typing import Dict, Union, Optional

#from Helper import basic_fnc

from Init.Functions.Logs import GetLogDef
from Lib.RDB import pyMysqlConnector
from bs4 import BeautifulSoup

from Realty.Government.Init import init_conf
from Realty.Government.Const import ConstRealEstateTable_GOV


from Realty.Auction.Const import ConstRealEstateTable_AUC
from datetime import datetime as DateTime, timedelta as TimeDelta
from Lib.SeleniumModule.Windows import Chrome
from selenium.webdriver.support.select import Select
from Realty.Auction.Const import AuctionCourtInfo
from Init.DefConstant import ConstRealEstateTable
from Realty.Naver.NaverLib import LibNaverMobileMasterSwitchTable
from Lib.CustomException import QuitException
from Lib.CryptoModule import AesCrypto

def main():

    try:
        print(GetLogDef.lineno(__file__), "============================================================")


        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        qrySelectCourtAuctionBackup = " SELECT *  FROM kt_realty_court_auction_backup "
        # qrySelectCourtAuctionBackup += " WHERE unique_value='VeM8u/ymP59Whaf6a/axNSPMpaWaoeXQsoltvwkeTYGQLB60saWH6nKvtWwkCSm4DHhcwVek5EzSYk9ZtU0lKg=='"

        cursorRealEstate.execute(qrySelectCourtAuctionBackup)
        rstFieldsLists = cursorRealEstate.fetchall()
        nLoop = 0

        for SelectColumnList in rstFieldsLists:

            jsonAddressData = SelectColumnList.get('address_data')
            strSequence = str(SelectColumnList.get('seq'))
            strLowerPrice = str(SelectColumnList.get('lower_price'))
            strRegDate = str(SelectColumnList.get('reg_date'))


            nAuctionCode = str(SelectColumnList.get('auction_code'))
            nAuctionSeq = str(SelectColumnList.get('auction_seq'))
            strCourtName = str(SelectColumnList.get('court_name'))
            dtAuctionDay = str(SelectColumnList.get('auction_day'))
            strAuctionType = str(SelectColumnList.get('auction_type'))

            strUniqueValue = nAuctionCode + "_" + nAuctionSeq + "_" + strCourtName + dtAuctionDay
            strUniqueValue2 = nAuctionCode + "_" + nAuctionSeq + "_" + strCourtName + dtAuctionDay + "_" + strAuctionType
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno), type(strUniqueValue), len(strUniqueValue), strUniqueValue)
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno), type(strUniqueValue2), len(strUniqueValue2), strUniqueValue2)

            aes = AesCrypto.AESCipher('aesKey')
            strUniqueValueEnc = aes.encrypt(strUniqueValue)
            strUniqueValue2Enc = aes.encrypt(strUniqueValue2)
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno), type(strUniqueValueEnc), len(strUniqueValueEnc), strUniqueValueEnc)
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno), type(strUniqueValue2Enc), len(strUniqueValue2Enc), strUniqueValue2Enc)

            qryInsertStatisticsTable  = " UPDATE kt_realty_court_auction_backup SET "
            qryInsertStatisticsTable += " unique_value = %s "
            qryInsertStatisticsTable += ", unique_value2 = %s "
            qryInsertStatisticsTable += " WHERE seq = '"+strSequence+"'"
            qryInsertStatisticsTable += " AND lower_price = '" + strLowerPrice + "'"
            qryInsertStatisticsTable += " AND reg_date = '" + strRegDate + "'"


            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno), qryInsertStatisticsTable, strUniqueValueEnc, strUniqueValue2Enc)



            cursorRealEstate.execute(qryInsertStatisticsTable, (strUniqueValueEnc, strUniqueValue2Enc))
            ResRealEstateConnection.commit()
            nLoop += 1
            print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno), type(nLoop), nLoop)




            # if len(jsonAddressData) > 2:
            #     print(GetLogDef.lineno(__file__), len(jsonAddressData), type(jsonAddressData), jsonAddressData)
            #     print(GetLogDef.lineno(__file__), type(SelectColumnList), SelectColumnList)



            # qryInsertStatisticsTable  = " UPDATE kt_realty_court_auction_backup_temp SET "
            # qryInsertStatisticsTable += " unique_value = %s "
            # qryInsertStatisticsTable += " WHERE seq = '"+strSequence+"'"
            #
            # cursorRealEstate.execute(qryInsertStatisticsTable, (strUniqueValue))
            # ResRealEstateConnection.commit()
            # nLoop += 1
            # print(GetLogDef.lineno(__file__), type(nLoop), nLoop)



    except QuitException as e:
        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)


        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno), "QuitException")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))

    except Exception as e:
        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)

        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno), "Error Exception")
        print(GetLogDef.GerLine(inspect.getframeinfo(inspect.currentframe()).filename,
                                                       inspect.getframeinfo(inspect.currentframe()).lineno), e, type(e))
        err_msg = traceback.format_exc()
        print(err_msg)

    else:
        print(GetLogDef.lineno(__file__), "[", nLoop, "]", "============================================================")

    finally:
        print("Finally END")



if __name__ == "__main__":
    main()