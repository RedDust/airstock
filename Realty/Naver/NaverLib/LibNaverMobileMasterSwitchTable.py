
import os
import sys
import pymysql
import time
import datetime

from Lib.RDB import pyMysqlConnector
from Init.DefConstant import ConstRealEstateTable
from Init.Functions.Logs import GetLogDef

def SwitchResultUpdate(SwitchData):


    try:

        SwitchData = str(SwitchData)
        dtNowDateTime = datetime.datetime.now()


        if SwitchData=='00' or SwitchData=='10' or SwitchData=='20':
            print(GetLogDef.lineno(), "[", str(datetime.datetime.now()), "]", SwitchData)

        else:
            Exception(GetLogDef.lineno(), 'SwitchData Not Allow')  # 예외를 발생시킴

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        if SwitchData=='10':
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.NaverMobileMasterSwitchTable + " SET " \
                                                                                              "result = %s" \
                                                                                              ",process_start_date=NOW() " \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  type='00'"

            print("[", str(datetime.datetime.now()), "]", sqlUpdateSwitch, SwitchData)

        else:
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.NaverMobileMasterSwitchTable + " SET " \
                                                                                              "result = %s" \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  type='00'"
            print("[", str(datetime.datetime.now()), "]", sqlUpdateSwitch, SwitchData)


        cursorRealEstate.execute(sqlUpdateSwitch, SwitchData)

    except Exception as e:

        print("NaverMobileMasterSwitchTable Error Exception")
        print(sqlUpdateSwitch)
        print(e)
        print(type(e))
        print("[ERROR]========================================================")

        ResRealEstateConnection.rollback()
        return False

    else:
        ResRealEstateConnection.commit()
        return True




def SwitchResultDataDistribution(seq,SwitchData):

    try:

        SwitchData = str(SwitchData)
        seq = str(seq)

        dtNowDateTime = datetime.datetime.now()


        if SwitchData=='00' or SwitchData=='10' or SwitchData=='20':
            print(GetLogDef.lineno(), "[", str(datetime.datetime.now()), "]", SwitchData)

        else:
            Exception(GetLogDef.lineno(), 'SwitchData Not Allow')  # 예외를 발생시킴

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        if SwitchData=='10':
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.NaverMobileMasterSwitchTable + " SET " \
                                                                                              " masterCortarNo = %s" \
                                                                                              ",result = %s" \
                                                                                              ",process_start_date=NOW() " \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  type='10'"

            print(GetLogDef.lineno(), "[", str(datetime.datetime.now()), "]", sqlUpdateSwitch, seq, SwitchData)

        else:
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.NaverMobileMasterSwitchTable + " SET " \
                                                                                              "masterCortarNo = %s" \
                                                                                              ", result = %s" \
                                                                                              ", last_date=NOW() " \
                                                                                              " WHERE  type='10'"
            print(GetLogDef.lineno(), "[", str(datetime.datetime.now()), "]", sqlUpdateSwitch, seq, SwitchData)


        cursorRealEstate.execute(sqlUpdateSwitch, (seq, SwitchData))

    except Exception as e:

        print("NaverMobileMasterSwitchTable Error Exception")
        print(sqlUpdateSwitch)
        print(e)
        print(type(e))
        print("[ERROR]========================================================")

        ResRealEstateConnection.rollback()
        return False

    else:
        ResRealEstateConnection.commit()
        return True

