
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


def SwitchResultDataAuction(type,city,target,SwitchData):

    try:

        SwitchData = str(SwitchData)
        type = str(type)
        city = str(city)
        target = str(target)

        dtNowDateTime = datetime.datetime.now()


        if SwitchData=='00' or SwitchData=='10' or SwitchData=='20':
            print(GetLogDef.lineno(), "[", str(datetime.datetime.now()), "]", SwitchData)

        else:
            Exception(GetLogDef.lineno(), 'SwitchData Not Allow')  # 예외를 발생시킴

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        if SwitchData=='30':
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.NaverMobileMasterSwitchTable + " SET " \
                                                                                              " masterCortarNo = %s" \
                                                                                              ", atclNo = %s" \
                                                                                              ", page = %s" \
                                                                                              ",result = %s" \
                                                                                              ",process_start_date=NOW() " \
                                                                                              ",last_date=NOW() " \
                                                                                              " WHERE  type='20'"

            print(GetLogDef.lineno(), "[", str(datetime.datetime.now()), "]", sqlUpdateSwitch, type, SwitchData)

        else:
            sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.NaverMobileMasterSwitchTable + " SET " \
                                                                                              "masterCortarNo = %s" \
                                                                                              ", atclNo = %s" \
                                                                                              ", page = %s" \
                                                                                              ", result = %s" \
                                                                                              ", last_date=NOW() " \
                                                                                              " WHERE  type='20'"
            print(GetLogDef.lineno(), "[", str(datetime.datetime.now()), "]", sqlUpdateSwitch, type, SwitchData)


        print(GetLogDef.lineno(__file__), "==================================================================")
        print(type, city , target, SwitchData)


        cursorRealEstate.execute(sqlUpdateSwitch, (type, city , target, SwitchData))

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



# type 00:네이버 부동산 물건 수집 , 10:네이버 물건 테이블 분산 , 20 : 법원경매 물건 수집
# startFlag 가 있으면 start_date 기록 1: start_data 기록 , 그외 기록하지 않음
# dictUpdateData = 업데이트 내용
def SwitchResultSelectV2(strType):

    try:
        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        #type 가  str 이 아니면 오류 방지
        if type(strType) is not str:
            strType = str(strType)

        # 스위치 데이터 조회 (10:처리중, 00:시작전, 20:오류 , 30:시작준비)
        qrySelectNaverMobileMaster = "SELECT * FROM " + ConstRealEstateTable.RealSwitchTable + "  WHERE switch_type=%s"

        cursorRealEstate.execute(qrySelectNaverMobileMaster, strType)
        results = cursorRealEstate.fetchone()

        return results

    except Exception as e:

        print("SwitchResultSelectV2 Error Exception")
        print("Select NaverMobileMasterSwitchTable ")
        print(e)
        print(type(e))
        print("[ERROR]========================================================")
        return False

    finally:
        # DB Close
        ResRealEstateConnection.close()



# type 00:네이버 부동산 물건 수집 , 10:네이버 물건 테이블 분산 , 20 : 법원경매 물건 수집
# startFlag 가 있으면 start_date 기록 True: start_data 기록 , 그외 기록하지 않음
# dictUpdateData = 업데이트 내용
def SwitchResultUpdateV2(strType,startFlag,dictUpdateData=dict()):

    try:

        #startFlag 가  str 이 아니면 오류 방지
        if type(strType) is not str:
            strType = str(strType)


        #startFlag 가  str 이 아니면 오류 방지
        if type(startFlag) is not bool:
            Exception(GetLogDef.lineno(), 'startFlag Dict Is Wrong')  # 예외를 발생시킴

        if type(dictUpdateData) is not dict:
            Exception(GetLogDef.lineno(), 'SwitchData Dict Is Wrong')  # 예외를 발생시킴

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.RealSwitchTable + " SET "

        if startFlag==True:
            sqlUpdateSwitch = sqlUpdateSwitch + ' process_start_date=NOW() , '

        for strKey, strUpdateData in dictUpdateData.items():

            if type(strUpdateData) is not str:
                strUpdateData = str(strUpdateData)

            sqlUpdateSwitch = sqlUpdateSwitch + strKey + "='"+strUpdateData+"', "

        sqlUpdateSwitch = sqlUpdateSwitch + ' last_date=NOW() '
        sqlUpdateSwitch = sqlUpdateSwitch + " WHERE  switch_type=%s"

        cursorRealEstate.execute(sqlUpdateSwitch , strType)


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
    finally:
        ResRealEstateConnection.close()





# type 00:네이버 부동산 물건 수집 , 10:네이버 물건 테이블 분산 , 20 : 법원경매 물건 수집
# startFlag 가 있으면 start_date 기록 True: start_data 기록 , 그외 기록하지 않음
# dictUpdateData = 업데이트 내용
def SwitchResultUpdateV3(strType,CStepFlag,dictUpdateData=dict()):

    try:

        #startFlag 가  str 이 아니면 오류 방지
        if type(strType) is not str:
            strType = str(strType)


        #startFlag 가  str 이 아니면 오류 방지
        if CStepFlag not in ['a','b','c','d']:
            Exception(GetLogDef.lineno() + 'CStepFlag =>' + CStepFlag)  # 예외를 발생시킴

        if type(dictUpdateData) is not dict:
            Exception(GetLogDef.lineno(), 'SwitchData Dict Is Wrong')  # 예외를 발생시킴

        # DB 연결
        ResRealEstateConnection = pyMysqlConnector.ResKtRealEstateConnection()
        cursorRealEstate = ResRealEstateConnection.cursor(pymysql.cursors.DictCursor)

        sqlUpdateSwitch = "UPDATE " + ConstRealEstateTable.RealSwitchTable + " SET "

        if CStepFlag=='a':
            sqlUpdateSwitch = sqlUpdateSwitch + ' process_start_date=NOW() , '

        for strKey, strUpdateData in dictUpdateData.items():

            if type(strUpdateData) is not str:
                strUpdateData = str(strUpdateData)

            sqlUpdateSwitch = sqlUpdateSwitch + strKey + "='"+strUpdateData+"', "

        sqlUpdateSwitch = sqlUpdateSwitch + ' last_date=NOW() '
        sqlUpdateSwitch = sqlUpdateSwitch + " WHERE  switch_type=%s"

        cursorRealEstate.execute(sqlUpdateSwitch , strType)


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
    finally:
        ResRealEstateConnection.close()

