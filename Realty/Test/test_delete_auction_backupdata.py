#test_delete_auction_backupdata

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


        encodedString = 'VeM8u/ymP59Whaf6a/axNSPMpaWaoeXQsoltvwkeTYGQLB60saWH6nKvtWwkCSm4DHhcwVek5EzSYk9ZtU0lKg=='

        encodedString2 = 'VeM8u/ymP59Whaf6a/axNSPMpaWaoeXQsoltvwkeTYGQLB60saWH6nKvtWwkCSm46nKSw5ypU5fjgYRu5eB6Hg=='

        originString = '20220130001456' + '7' + '2023-09-13 00:00:00'

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        qrySelectCourtAuctionBackupTotal = " SELECT * " +ConstRealEstateTable_AUC.CourtAuctionBackupTable +" "
        qrySelectCourtAuctionBackupTotal += " WHERE unique_value='"+encodedString+"'"
        # qrySelectCourtAuctionBackupTotal += " limit 100"

        aes = AesCrypto.AESCipher('aesKey')

        strUniqueValueEnc = aes.encrypt(originString)
        print(strUniqueValueEnc)



        strUniqueValueEnc = aes.decrypt(encodedString)
        print(strUniqueValueEnc)

        strUniqueValueEnc = aes.decrypt(encodedString2)
        print(strUniqueValueEnc)



    except QuitException as e:
        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)


        print(GetLogDef.lineno(__file__), "QuitException")
        err_msg = traceback.format_exc()
        print(err_msg)
        print(e)
        print(type(e))

    except Exception as e:
        # 스위치 데이터 업데이트 (10:처리중, 00:시작전, 20:오류 , 30:시작준비 - start_time 기록)

        print(GetLogDef.lineno(__file__), "Error Exception")
        print(GetLogDef.lineno(__file__), e, type(e))
        err_msg = traceback.format_exc()
        print(err_msg)

    else:

        print(GetLogDef.lineno(__file__),"[","]" ,"============================================================")

    finally:

        print("Finally END")



if __name__ == "__main__":
    main()