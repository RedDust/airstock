import sys
import pymysql
import time
import datetime

from Lib.RDB import pyMysqlConnector
from Init.DefConstant import ConstRealEstateTable
from Init.Functions.Logs import GetLogDef

def SwitchSeoulUpdate(dictSeoulSwitch):


    try:

        dtNowDateTime = datetime.datetime.now()
        nSequence = str(dictSeoulSwitch['seq'])
        SwitchData = str(dictSeoulSwitch['state'])

        if SwitchData=='00' or SwitchData=='10' or SwitchData=='20' or SwitchData=='30':
            print(GetLogDef.lineno(), "[", str(datetime.datetime.now()), "]", SwitchData)

        else:
            Exception(GetLogDef.lineno(), 'SwitchData Not Allow')  # 예외를 발생시킴

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        if SwitchData=='10':
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.SeoulRealTradeMasterSwitchTable + " SET " \
                                                                                              "state = %s" \
                                                                                              ",process_start_date=NOW() " \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  seq='"+nSequence+"' "

        elif SwitchData=='30':

            nProcessedCount = str(dictSeoulSwitch['processed_count'])

            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.SeoulRealTradeMasterSwitchTable + " SET " \
                                                                                              "state = %s" \
                                                                                              ",process_start_date=NOW() " \
                                                                                             ",processed_count='"+nProcessedCount+"' " \
                                                                                             ",last_date=NOW() " \
                                                                                              " WHERE  seq='"+nSequence+"' "

        else:
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.SeoulRealTradeMasterSwitchTable + " SET " \
                                                                                              "state = %s" \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  seq='"+nSequence+"' "


        print("[", str(datetime.datetime.now()), "]", sqlUpdateSwitch, "SwitchData > ", SwitchData)

        cursorRealEstate.execute(sqlUpdateSwitch, SwitchData)

    except Exception as e:

        print("SwitchSeoulUpdate Error Exception")
        print(sqlUpdateSwitch)
        print(e)
        print(type(e))
        print("[ERROR]========================================================")

        ResRealEstateConnection.rollback()
        return False

    else:
        ResRealEstateConnection.commit()
        return True



def SwitchSeoulRentUpdate(dictSeoulSwitch):


    try:

        dtNowDateTime = datetime.datetime.now()
        nSequence = str(dictSeoulSwitch['seq'])
        SwitchData = str(dictSeoulSwitch['state'])

        if SwitchData=='00' or SwitchData=='10' or SwitchData=='20' or SwitchData=='30':
            print(GetLogDef.lineno(), "[", str(datetime.datetime.now()), "]", SwitchData)

        else:
            Exception(GetLogDef.lineno(), 'SwitchData Not Allow')  # 예외를 발생시킴

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        if SwitchData=='10':
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.SeoulRealRentMasterSwitchTable + " SET " \
                                                                                              "state = %s" \
                                                                                              ",process_start_date=NOW() " \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  seq='"+nSequence+"' "

        elif SwitchData=='30':

            nProcessedCount = str(dictSeoulSwitch['processed_count'])

            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.SeoulRealRentMasterSwitchTable + " SET " \
                                                                                              "state = %s" \
                                                                                              ",process_start_date=NOW() " \
                                                                                             ",processed_count='"+nProcessedCount+"' " \
                                                                                             ",last_date=NOW() " \
                                                                                              " WHERE  seq='"+nSequence+"' "

        else:
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.SeoulRealRentMasterSwitchTable + " SET " \
                                                                                              "state = %s" \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  seq='"+nSequence+"' "


        print("[", str(datetime.datetime.now()), "]", sqlUpdateSwitch, "SwitchData > ", SwitchData)

        cursorRealEstate.execute(sqlUpdateSwitch, SwitchData)

    except Exception as e:

        print("SwitchSeoulUpdate Error Exception")
        print(sqlUpdateSwitch)
        print(e)
        print(type(e))
        print("[ERROR]========================================================")

        ResRealEstateConnection.rollback()
        return False

    else:
        ResRealEstateConnection.commit()
        return True
