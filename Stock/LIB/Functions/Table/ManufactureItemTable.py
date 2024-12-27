import os
import sys
import pymysql
import time
import datetime
import traceback
import inspect as Isp
from datetime import datetime as DateTime, timedelta as TimeDelta
from Stock.LIB.RDB import pyMysqlConnector
from Stock.CONFIG import ConstTableName
from Stock.CONFIG import KoreaInvestTableName

def GetItemTableName(item_code,country,database='stockfriends'):
    from Init.Functions.Logs import GetLogDef as SLog
    from Stock.LIB.Logging import UnifiedLogDeclarationFunction as ULF
    strNewPartTable = ''
    try:
        if len(item_code) != 6:
            raise Exception("GetItemTableName ERROR item_code " + item_code)  # 예외를 발생시킴

        if len(country) < 1:
            raise Exception("GetItemTableName ERROR country" + country)  # 예외를 발생시킴

        if database == 'stockfriends':
            strNewPartTableName = ConstTableName.StockItemRootTable + "_" + item_code  + "_" + country
        elif database == 'koreainvest':
            strNewPartTableName = KoreaInvestTableName.StockItemRootTable + "_" + item_code  + "_" + country
        else:
            raise Exception("GetItemTableName ERROR country" + country)  # 예외를 발생시킴


    except Exception as e:
        strReturn=''
    else:
        strReturn = strNewPartTableName

    finally:
        print("GetItemTableName finally strNewPartTableName =>[", strNewPartTableName, "]")

    return strReturn


def CheckExistItemTable(strNewPartTable , ResItemsConnection,database='stockfriends'):
    # # DB 연결
    bResult = False

    try:
        cursorItems = ResItemsConnection.cursor(pymysql.cursors.DictCursor)

        print("cursorItems=> " , cursorItems)

        if len(strNewPartTable) < 1:
            raise Exception("CheckExistItemTable ERROR strNewPartTable" + strNewPartTable)  # 예외를 발생시킴

        if database == 'stockfriends':
            qryCheckTable = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA='" + ConstTableName.StockFriendsDatabase + "' AND TABLE_NAME='" + strNewPartTable + "'"
            cursorItems.execute(qryCheckTable)
            results = cursorItems.rowcount
            print("qryCheckTable =>[", qryCheckTable, "]")
            if results < 1:
                qryCreateBackupTable = "create table " + strNewPartTable + " LIKE " + ConstTableName.StockItemOriginTable
                print("qryCreateBackupTable =>[", qryCreateBackupTable, "]")
                cursorItems.execute(qryCreateBackupTable)
                ResItemsConnection.commit()

        elif database == 'koreainvest':

            qryCheckTable = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA='" + KoreaInvestTableName.DatabaseName + "' AND TABLE_NAME='" + strNewPartTable + "'"
            cursorItems.execute(qryCheckTable)
            results = cursorItems.rowcount
            print("qryCheckTable =>[", qryCheckTable, "]")
            if results < 1:
                qryCreateBackupTable = "create table " + strNewPartTable + " LIKE " + KoreaInvestTableName.StockItemOriginTable
                print("qryCreateBackupTable =>[", qryCreateBackupTable, "]")
                cursorItems.execute(qryCreateBackupTable)
                ResItemsConnection.commit()
        else:
            raise Exception("GetItemTableName ERROR strNewPartTable" + strNewPartTable)  # 예외를 발생시킴




    except Exception as e:
        print("qryCheckTable =>[", qryCheckTable, "]")
        print("results =>[",results,"]")
    else:
        print("qryCheckTable =>[", qryCheckTable, "]")
        print("results =>[", results, "]")
        bResult = True
    finally:
        print("qryCheckTable =>[", qryCheckTable, "]")
        print("results =>[", results, "]")

        return bResult






# def MakeItemTable(item_code , country):
#     qryCheckTable = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA='" + ConstRealEstateTable_GOV.RealtyDatabase + "' AND TABLE_NAME='" + strNewPartTable + "'"
#     cursorRealEstate.execute(qryCheckTable)
#
#     qryCheckTable = "SELECT count(*) FROM information_schema.tables WHERE TABLE_SCHEMA='" + ConstRealEstateTable.RealtyDatabase + "' AND TABLE_NAME='" + strNewPartTable + "'"
#     cursorRealEstate.execute(qryCheckTable)
#     results = cursorRealEstate.rowcount
#     if results < 1:
#         strNewPartTable = ConstRealEstateTable.NaverMobileMasterTable + "_ETC"
#
#
#     qryCreateBackupTable = "create table " + strNewPartTable + " LIKE " + ConstRealEstateTable_GOV.MolitRealTradeBasicAPTRentStatisticsMasterTable
#     print(GetLogDef.lineno(__file__), "qryCreateBackupTable > ", qryCreateBackupTable)
#     cursorRealEstate.execute(qryCreateBackupTable)
