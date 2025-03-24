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


        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        # qrySelectCourtAuctionBackup = " SELECT unique_value FROM kt_realty_court_auction_backup_temp "
        # qrySelectCourtAuctionBackup += " group by  unique_value order by 2 desc"
        # qrySelectCourtAuctionBackup += " limit 100"


        qrySelectCourtAuctionBackup = " SELECT * FROM kt_realty_court_auction_backup "
        # qrySelectCourtAuctionBackup += " WHERE auction_code='20220130015450' "

        # qrySelectCourtAuctionBackup += " limit 10000"

        cursorRealEstate.execute(qrySelectCourtAuctionBackup)
        rstFieldsLists = cursorRealEstate.fetchall()
        nLoop = 0

        dictAddressDatas = dict()

        for SelectColumnList in rstFieldsLists:

            jsonAddressData = str(SelectColumnList.get('address_data'))
            strAddressDataText = str(SelectColumnList.get('address_data_text'))
            strSequence = str(SelectColumnList.get('seq'))

            strAuctionCode = str(SelectColumnList.get('auction_code'))
            strAuctionSeq = str(SelectColumnList.get('auction_seq'))
            strCourtName = str(SelectColumnList.get('court_name'))



            dtAuctionDay = str(SelectColumnList.get('auction_day'))
            strAuctionType = str(SelectColumnList.get('auction_type'))
            strUniqueValue = SelectColumnList.get('unique_value')
            strUniqueValue2 = SelectColumnList.get('unique_value2')





            if len(strAddressDataText) <= 2:
                print(GetLogDef.lineno(__file__), '-------------------------------------------------------------')
                print(GetLogDef.lineno(__file__), type(strSequence), len(strSequence), strSequence)
                print(GetLogDef.lineno(__file__), type(strUniqueValue), len(strUniqueValue), strUniqueValue)

                qrySelectBackupList = " SELECT * FROM kt_realty_court_auction_backup "
                qrySelectBackupList += " WHERE auction_code = %s AND auction_seq =%s AND court_name=%s "
                cursorRealEstate.execute(qrySelectBackupList, (strAuctionCode, strAuctionSeq, strCourtName))
                rstBackupLists = cursorRealEstate.fetchall()

                for rstBackupList in rstBackupLists:
                    jsonBackAddressData = rstBackupList.get('address_data')

                    if len(jsonBackAddressData) > 5:
                        # print(GetLogDef.lineno(__file__), '-------------------------------------------------------------')
                        print(GetLogDef.lineno(__file__), type(strSequence), len(strSequence), strSequence)
                        print(GetLogDef.lineno(__file__), ">>" , type(jsonBackAddressData), len(jsonBackAddressData), jsonBackAddressData)

                        qryUpdateBackup = " UPDATE kt_realty_court_auction_backup SET "
                        qryUpdateBackup += " address_data = %s"
                        qryUpdateBackup += ", address_data_text = %s"
                        qryUpdateBackup += " WHERE seq = %s"
                        cursorRealEstate.execute(qryUpdateBackup, (jsonBackAddressData, jsonBackAddressData, strSequence))
                        ResRealEstateConnection.commit()
                        break







        #
        #
        #     qrySelectBackupList  = " SELECT * FROM kt_realty_court_auction_backup_temp "
        #     qrySelectBackupList += " WHERE unique_value = '"+strUniqueValue+"' order by address_data desc  "
        #     cursorRealEstate.execute(qrySelectBackupList)
        #     rstBackupLists = cursorRealEstate.fetchall()
        #
        #     for rstBackupList in rstBackupLists:
        #
        #         strSequence = str(rstBackupList.get('seq'))
        #         jsonAddressData = rstBackupList.get('address_data')
        #         nAuctionCode = str(rstBackupList.get('auction_code'))
        #         nAuctionSeq = str(rstBackupList.get('auction_seq'))
        #         strCourtName = str(rstBackupList.get('court_name'))
        #         dtAuctionDay = str(rstBackupList.get('auction_day'))
        #         strBackupUniqueValue = rstBackupList.get('unique_value')
        #
        #         if len(jsonAddressData) <= 2:
        #             print(GetLogDef.lineno(__file__), '-------------------------------------------------------------')
        #             print(GetLogDef.lineno(__file__), type(strSequence), len(strSequence), strSequence)
        #             print(GetLogDef.lineno(__file__), type(dictAddressDatas[strBackupUniqueValue]), len(dictAddressDatas[strBackupUniqueValue]), dictAddressDatas[strBackupUniqueValue])
        #
        #             qrySelectBackupList = " SELECT * FROM kt_realty_court_auction_backup_temp "
        #             qrySelectBackupList += " WHERE unique_value = '" + strUniqueValue + "'  "
        #
        #
        #
        #
        #
        #
        #     # intCount = int(SelectColumnList.get('cnt'))
        #     # print(GetLogDef.lineno(__file__),type(strUniqueValue), len(strUniqueValue), strUniqueValue)
        #     # print(GetLogDef.lineno(__file__), type(intCount), intCount)
        #
        #     # qrySelectBackupList  = " SELECT * FROM kt_realty_court_auction_backup_temp "
        #     # qrySelectBackupList += " WHERE unique_value = '"+strUniqueValue+"' order by address_data desc  "
        #     cursorRealEstate.execute(qrySelectBackupList)
        #     rstBackupLists = cursorRealEstate.fetchall()
        #
        #     for rstBackupList in rstBackupLists:
        #         # print(GetLogDef.lineno(__file__), type(rstBackupList), len(rstBackupList), rstBackupList)
        #
        #         strSequence = str(rstBackupList.get('seq'))
        #         jsonAddressData = rstBackupList.get('address_data')
        #         nAuctionCode = str(rstBackupList.get('auction_code'))
        #         nAuctionSeq = str(rstBackupList.get('auction_seq'))
        #         strCourtName = str(rstBackupList.get('court_name'))
        #         dtAuctionDay = str(rstBackupList.get('auction_day'))
        #         strBackupUniqueValue = rstBackupList.get('unique_value')
        #
        #
        #         # print(GetLogDef.lineno(__file__), type(jsonAddressData), len(jsonAddressData), jsonAddressData)
        #
        #         if len(jsonAddressData) <= 2:
        #             print(GetLogDef.lineno(__file__), '-------------------------------------------------------------')
        #             print(GetLogDef.lineno(__file__), type(strSequence), len(strSequence), strSequence)
        #             print(GetLogDef.lineno(__file__), type(dictAddressDatas[strBackupUniqueValue]), len(dictAddressDatas[strBackupUniqueValue]), dictAddressDatas[strBackupUniqueValue])
        #
        #             qrySelectBackupList = " SELECT * FROM kt_realty_court_auction_backup_temp "
        #             qrySelectBackupList += " WHERE unique_value = '" + strUniqueValue + "'  "
        #
        #
        #
        #
        #             qryUpdateBackup = " UPDATE kt_realty_court_auction_backup_temp SET "
        #             qryUpdateBackup += " address_data = %s"
        #             qryUpdateBackup += ", address_data_text = %s"
        #             qryUpdateBackup += " WHERE seq = %s"
        #
        #             cursorRealEstate.execute(qryUpdateBackup, ( dictAddressDatas[strBackupUniqueValue] , dictAddressDatas[strBackupUniqueValue], strSequence ))
        #             ResRealEstateConnection.commit()
        #
        #
        #
        # print(dictAddressDatas)



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

        print(GetLogDef.lineno(__file__),"[", nLoop,"]" ,"============================================================")

    finally:

        print("Finally END")



if __name__ == "__main__":
    main()